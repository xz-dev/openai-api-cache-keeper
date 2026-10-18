package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

type keeper struct {
	cfg    config
	store  *store
	client *http.Client
}

func newKeeper(cfg config) *keeper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true // SSE must arrive as identity so events can be parsed
	return &keeper{
		cfg:    cfg,
		store:  newStore(cfg.ttl, cfg.maxStoreBytes),
		client: &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
}

func (k *keeper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
		return
	}
	if r.URL.Path != "/v1/responses" && r.URL.Path != "/responses" {
		writeAPIError(w, &apiError{status: http.StatusNotFound, typ: "invalid_request_error", message: "not found"})
		return
	}
	switch {
	case r.Method == http.MethodGet && strings.EqualFold(r.Header.Get("Upgrade"), "websocket"):
		k.serveWS(w, r)
	case r.Method == http.MethodPost:
		k.serveHTTP(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeAPIError(w, &apiError{status: http.StatusMethodNotAllowed, typ: "invalid_request_error", message: "method not allowed"})
	}
}

// apiError renders as an OpenAI-shaped error object.
type apiError struct {
	status                    int
	typ, code, param, message string
}

func (e *apiError) object() map[string]any {
	obj := map[string]any{"type": e.typ, "message": e.message, "code": nil, "param": nil}
	if e.code != "" {
		obj["code"] = e.code
	}
	if e.param != "" {
		obj["param"] = e.param
	}
	return obj
}

func writeAPIError(w http.ResponseWriter, e *apiError) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": e.object()})
}

func ownerOf(r *http.Request) [32]byte { return sha256.Sum256([]byte(r.Header.Get("Authorization"))) }

// turn is one prepared request: the body to send upstream and the full input
// that body carries, which becomes the stored history once the turn completes.
type turn struct {
	body  []byte
	input []json.RawMessage
	store bool
}

type lookupFunc func(id string) ([]json.RawMessage, bool)

// prepare expands previous_response_id into the full input history and drops
// the field, so the upstream never needs to hold state.
func prepare(doc map[string]json.RawMessage, lookup lookupFunc) (*turn, *apiError) {
	input, err := inputItems(doc["input"])
	if err != nil {
		return nil, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", param: "input", message: err.Error()}
	}
	if raw, ok := doc["previous_response_id"]; ok && string(raw) != "null" {
		var id string
		if json.Unmarshal(raw, &id) != nil || id == "" {
			return nil, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", param: "previous_response_id", message: "previous_response_id must be a non-empty string"}
		}
		history, found := lookup(id)
		if !found {
			return nil, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", code: "previous_response_not_found", param: "previous_response_id", message: fmt.Sprintf("Previous response with id '%s' not found.", id)}
		}
		input = append(append(make([]json.RawMessage, 0, len(history)+len(input)), history...), input...)
	}
	delete(doc, "previous_response_id")
	inputRaw, err := json.Marshal(input)
	if err != nil {
		return nil, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", param: "input", message: "input is not valid JSON"}
	}
	doc["input"] = inputRaw
	body, err := json.Marshal(doc)
	if err != nil {
		return nil, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", message: "request is not valid JSON"}
	}
	return &turn{body: body, input: input, store: string(doc["store"]) != "false"}, nil
}

// inputItems normalizes Responses input to a list of independent item copies.
func inputItems(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	var text string
	if json.Unmarshal(raw, &text) == nil {
		item, _ := json.Marshal(map[string]string{"role": "user", "content": text})
		return []json.RawMessage{item}, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, errors.New("input must be a string or an array of items")
	}
	for i, item := range items {
		items[i] = append(json.RawMessage(nil), item...)
	}
	return items, nil
}

// collector reconstructs the final response from SSE events or a JSON body.
type collector struct {
	items  []json.RawMessage // response.output_item.done, by output_index
	id     string
	status string
	output []json.RawMessage
	done   bool
}

func (c *collector) event(data []byte) {
	var ev struct {
		Type        string          `json:"type"`
		OutputIndex *int            `json:"output_index"`
		Item        json.RawMessage `json:"item"`
		Response    json.RawMessage `json:"response"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return
	}
	switch ev.Type {
	case "response.output_item.done":
		if ev.OutputIndex != nil && *ev.OutputIndex >= 0 && *ev.OutputIndex < 4096 && len(ev.Item) > 0 {
			for len(c.items) <= *ev.OutputIndex {
				c.items = append(c.items, nil)
			}
			c.items[*ev.OutputIndex] = append(json.RawMessage(nil), ev.Item...)
		}
	case "response.completed", "response.incomplete":
		c.response(ev.Response)
	}
}

func (c *collector) response(raw []byte) {
	var resp struct {
		ID     string            `json:"id"`
		Status string            `json:"status"`
		Output []json.RawMessage `json:"output"`
	}
	if json.Unmarshal(raw, &resp) != nil || resp.ID == "" {
		return
	}
	c.id, c.status, c.done = resp.ID, resp.Status, true
	c.output = make([]json.RawMessage, 0, len(resp.Output))
	for _, item := range resp.Output {
		c.output = append(c.output, append(json.RawMessage(nil), item...))
	}
}

// final returns the response id and output items when the turn may be continued.
func (c *collector) final() (string, []json.RawMessage, bool) {
	if !c.done || (c.status != "completed" && c.status != "incomplete") {
		return "", nil, false
	}
	output := c.output
	if len(output) == 0 { // some upstreams only stream items and send an empty final output
		for _, item := range c.items {
			if item != nil {
				output = append(output, item)
			}
		}
	}
	return c.id, output, true
}

// publicID is the id clients see and continue from. Upstream ids that do not
// look like response ids (e.g. Anthropic-style msg_*) get a resp_ prefix, since
// gateways such as Sub2API reject message-like previous_response_id values.
func publicID(id string) string {
	if strings.HasPrefix(id, "resp_") {
		return id
	}
	return "resp_" + id
}

// rewriteResponse applies publicID to the response object of one event or JSON
// body and, when the keeper stores the turn, declares store:true: the keeper is
// the store, whatever the stateless upstream reported. Payloads that need no
// change are returned unchanged.
func rewriteResponse(payload []byte, stored bool) []byte {
	if !bytes.Contains(payload, []byte(`"id"`)) {
		return payload
	}
	var doc map[string]json.RawMessage
	if json.Unmarshal(payload, &doc) != nil {
		return payload
	}
	if nested, ok := doc["response"]; ok {
		if fixed, changed := rewriteObject(nested, stored); changed {
			doc["response"] = fixed
			if out, err := json.Marshal(doc); err == nil {
				return out
			}
		}
		return payload
	}
	if string(doc["object"]) == `"response"` {
		if fixed, changed := rewriteObject(payload, stored); changed {
			return fixed
		}
	}
	return payload
}

func rewriteObject(raw json.RawMessage, stored bool) (json.RawMessage, bool) {
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil {
		return raw, false
	}
	var id string
	if json.Unmarshal(obj["id"], &id) != nil || id == "" {
		return raw, false
	}
	changed := false
	if publicID(id) != id {
		obj["id"], _ = json.Marshal(publicID(id))
		changed = true
	}
	if stored && string(obj["store"]) != "true" {
		obj["store"] = json.RawMessage("true")
		changed = true
	}
	if !changed {
		return raw, false
	}
	out, err := json.Marshal(obj)
	if err != nil {
		return raw, false
	}
	return out, true
}

// rewriteSSELine applies rewriteResponse to a single-line SSE data field.
func rewriteSSELine(line []byte, stored bool) []byte {
	if !bytes.HasPrefix(line, []byte("data:")) {
		return line
	}
	body := bytes.TrimRight(line[5:], "\r\n")
	ending := line[5+len(body):]
	payload := bytes.TrimPrefix(body, []byte(" "))
	fixed := rewriteResponse(payload, stored)
	if len(fixed) == len(payload) && bytes.Equal(fixed, payload) {
		return line
	}
	return append(append([]byte("data: "), fixed...), ending...)
}

// forward sends the prepared body to the same path upstream.
func (k *keeper) forward(ctx context.Context, path string, header http.Header, body []byte) (*http.Response, error) {
	target := *k.cfg.upstream
	target.Path = strings.TrimRight(target.Path, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	copyHeader(req.Header, header)
	for name := range req.Header {
		if strings.HasPrefix(strings.ToLower(name), "sec-websocket-") {
			req.Header.Del(name)
		}
	}
	req.Header.Del("Accept-Encoding")
	req.Header.Del("Content-Length")
	req.Header.Set("Content-Type", "application/json")
	return k.client.Do(req)
}

func (k *keeper) serveHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, k.cfg.maxRequestBytes))
	if err != nil {
		writeAPIError(w, &apiError{status: http.StatusRequestEntityTooLarge, typ: "invalid_request_error", message: "request body too large or unreadable"})
		return
	}
	var doc map[string]json.RawMessage
	if json.Unmarshal(raw, &doc) != nil || doc == nil {
		writeAPIError(w, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", message: "request body must be a JSON object"})
		return
	}
	owner := ownerOf(r)
	hasPrev := doc["previous_response_id"] != nil
	t, apiErr := prepare(doc, func(id string) ([]json.RawMessage, bool) { return k.store.get(id, owner) })
	if t != nil && k.cfg.ignoreClientStoreFalse {
		t.store = true
	}
	if apiErr != nil {
		writeAPIError(w, apiErr)
		logTurn("http", apiErr.status, hasPrev, "", start, k.store)
		return
	}
	resp, err := k.forward(r.Context(), r.URL.Path, r.Header, t.body)
	if err != nil {
		writeAPIError(w, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"})
		logTurn("http", http.StatusBadGateway, hasPrev, "", start, k.store)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	w.WriteHeader(resp.StatusCode)
	var c collector
	if resp.StatusCode == http.StatusOK && strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		rc := http.NewResponseController(w)
		_ = readSSE(resp.Body, func(line []byte) error {
			if _, err := w.Write(rewriteSSELine(line, t.store)); err != nil {
				return err
			}
			if len(bytes.TrimSpace(line)) == 0 {
				return rc.Flush()
			}
			return nil
		}, func(data []byte) error { c.event(data); return nil })
	} else {
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode == http.StatusOK {
			c.response(body)
			body = rewriteResponse(body, t.store)
		}
		_, _ = w.Write(body)
	}
	id := ""
	if rid, output, ok := c.final(); ok && t.store {
		id = publicID(rid)
		k.store.put(id, owner, append(t.input, output...))
	}
	logTurn("http", resp.StatusCode, hasPrev, id, start, k.store)
}

// readSSE passes every raw line to onLine (when set) and each complete event's
// data payload to onData. [DONE] sentinels are not reported as data.
func readSSE(body io.Reader, onLine func([]byte) error, onData func([]byte) error) error {
	reader := bufio.NewReaderSize(body, 64<<10)
	var data []byte
	dispatch := func() error {
		if len(data) == 0 {
			return nil
		}
		payload := data
		data = nil
		if string(payload) == "[DONE]" {
			return nil
		}
		return onData(payload)
	}
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if onLine != nil {
				if werr := onLine(line); werr != nil {
					return werr
				}
			}
			trimmed := bytes.TrimRight(line, "\r\n")
			switch {
			case len(trimmed) == 0:
				if derr := dispatch(); derr != nil {
					return derr
				}
			case bytes.HasPrefix(trimmed, []byte("data:")):
				value := bytes.TrimPrefix(trimmed[5:], []byte(" "))
				if data != nil {
					data = append(data, '\n')
				}
				data = append(data, value...)
			}
		}
		if err != nil {
			if derr := dispatch(); derr != nil {
				return derr
			}
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

var hopHeaders = []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

func copyHeader(dst, src http.Header) {
	clean := src.Clone()
	for _, name := range strings.Split(clean.Get("Connection"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			clean.Del(name)
		}
	}
	for _, name := range hopHeaders {
		clean.Del(name)
	}
	for name, values := range clean {
		dst[name] = values
	}
}

func logTurn(transport string, status int, hasPrev bool, storedID string, start time.Time, s *store) {
	entries, size := s.stats()
	log.Printf("transport=%s status=%d previous=%t stored=%t duration=%s store_entries=%d store_bytes=%d",
		transport, status, hasPrev, storedID != "", time.Since(start).Round(time.Millisecond), entries, size)
}

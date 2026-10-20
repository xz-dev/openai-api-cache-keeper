package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type keeper struct {
	cfg     config
	store   *store
	client  *http.Client
	routing *routingClient
	work    *workingSet
}

func newKeeper(cfg config) *keeper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true // SSE must arrive as identity so events can be parsed
	st, err := newStore(cfg.storeDir, cfg.ttl, cfg.maxStoreBytes, cfg.storeMetaBytes, cfg.storePinBytes, cfg.storeCompactBytes)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	if err := cleanupTransient(cfg.storeDir); err != nil {
		st.close()
		log.Fatalf("transient cleanup: %v", err)
	}
	var routing *routingClient
	if cfg.routingIndex != nil {
		routing = newRoutingClient(cfg.routingIndex)
	}
	return &keeper{
		routing: routing,
		work:    newWorkingSet(),
		cfg:     cfg,
		store:   st,
		client:  &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
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

func storageError() *apiError {
	return &apiError{status: http.StatusServiceUnavailable, typ: "server_error", code: "history_storage_failed", message: "Turn history could not be retained; previous accepted history is unchanged."}
}

func ownerOf(r *http.Request) [32]byte { return sha256.Sum256([]byte(r.Header.Get("Authorization"))) }

type lookupFunc func(id string) ([]json.RawMessage, bool)

// prepare expands previous_response_id into the full input history and drops
// the field, so the upstream never needs to hold state.
func expandInput(doc map[string]json.RawMessage, lookup lookupFunc) ([]json.RawMessage, *apiError) {
	input, err := inputItems(doc["input"])
	if err != nil {
		if errors.Is(err, errWorkSize) {
			return nil, workingError(err)
		}
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
	return input, nil
}

// inputItems normalizes Responses input to a list of independent item copies.
func inputItems(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' && json.Valid(raw) {
		item := append([]byte(`{"role":"user","content":`), raw...)
		item = append(item, '}')
		return []json.RawMessage{item}, nil
	}
	var items []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	start, err := dec.Token()
	if err != nil || start != json.Delim('[') {
		return nil, errors.New("input must be a string or an array of items")
	}
	for dec.More() {
		if len(items) >= maxHistoryItems {
			return nil, errWorkSize
		}
		var item json.RawMessage
		if err := dec.Decode(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return items, nil
}

// collector reconstructs the final response from SSE events or a JSON body.
type collector struct {
	charged int64
	items   []json.RawMessage // response.output_item.done, by output_index
	id      string
	status  string
	output  []json.RawMessage
	done    bool
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

func (k *keeper) forwardReader(ctx context.Context, upstream *url.URL, path string, header http.Header, body io.Reader) (*http.Response, error) {
	target := *upstream
	target.Path = strings.TrimRight(target.Path, "/") + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), body)
	if err != nil {
		if closer, ok := body.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, err
	}
	copyHeader(req.Header, header)
	if k.routing != nil && upstream == k.cfg.upstreamCPA {
		req.Header.Set("Authorization", "Bearer "+k.cfg.cpaAPIKey)
	}
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
	if !trySlot(k.work.turns) {
		writeAPIError(w, workingError(errors.New("turn limit")))
		return
	}
	defer func() { <-k.work.turns }()
	raw, err := k.spoolRequest(r.Context(), http.MaxBytesReader(w, r.Body, k.cfg.maxRequestBytes))
	if err != nil {
		writeAPIError(w, workingError(err))
		return
	}
	defer raw.Close()
	upstream, stream, routeErr := k.routeRequest(r.Context(), raw, false)
	if routeErr != nil {
		writeRoutingError(w, routeErr, stream)
		return
	}
	leave, err := k.work.enter(r.Context())
	if err != nil {
		writeAPIError(w, workingError(err))
		return
	}
	owner := ownerOf(r)
	t, apiErr := k.prepareDisk(r.Context(), raw, owner, func(id string) ([32]byte, bool) {
		tip, _, ok := k.store.resolveID(id, owner, false)
		return tip, ok
	}, false)
	leave()
	raw.Close()
	if apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	defer t.file.Close()
	hasPrev := t.hasPrev
	resp, err := k.forwardDisk(r.Context(), upstream, r.URL.Path, r.Header, t)
	if err != nil {
		writeAPIError(w, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"})
		logTurn("http", http.StatusBadGateway, hasPrev, "", start, k.store)
		return
	}
	defer resp.Body.Close()
	copyHeader(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	var c collector
	defer c.release(k.work)
	id := ""
	var acceptError *apiError
	accept := func() error {
		if rid, output, ok := c.final(); ok && t.store && id == "" {
			if err := r.Context().Err(); err != nil {
				return err
			}
			if _, err := k.acceptDisk(r.Context(), t, publicID(rid), owner, output, "", false); err != nil {
				acceptError = storageError()
				return err
			}
			id = publicID(rid)
		}
		return nil
	}
	if resp.StatusCode == http.StatusOK && strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		w.WriteHeader(resp.StatusCode)
		rc := http.NewResponseController(w)
		var wire []byte
		var release func()
		err = readSSE(resp.Body, func(frame []byte) error {
			if release != nil {
				defer release()
				defer func() { wire = nil; release = nil }()
			}
			if err := writeSSEFrame(w, frame, wire); err != nil {
				return err
			}
			return rc.Flush()
		}, func(data []byte) error {
			var e error
			wire, release, e = k.responseFrame(r.Context(), &c, data, true, t.store, accept)
			return e
		}, k.work)
		if err == nil && !c.done {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			log.Printf("http relay ended early: %v", err)
			if r.Context().Err() == nil && id == "" {
				failure := responseError(err)
				if acceptError != nil {
					failure = acceptError
				}
				payload, _ := json.Marshal(map[string]any{"type": "error", "error": failure.object()})
				_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", payload)
				_ = rc.Flush()
			}
		}
	} else {
		if resp.StatusCode != http.StatusOK {
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}
		body, err := readResponseBody(resp.Body, k.work)
		if err != nil {
			writeAPIError(w, responseError(err))
			return
		}
		defer body.close()
		wire, release, err := k.responseFrame(r.Context(), &c, body.data, false, t.store, accept)
		if err != nil {
			failure := responseError(err)
			if acceptError != nil {
				failure = acceptError
			}
			writeAPIError(w, failure)
			return
		}
		defer release()
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(wire)
	}
	logTurn("http", resp.StatusCode, hasPrev, id, start, k.store)
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

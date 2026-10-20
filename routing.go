package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	routingConnectTimeout = 300 * time.Millisecond
	routingSendTimeout    = 300 * time.Millisecond
	routingReadTimeout    = time.Second
	maxRoutingBody        = 1024
	maxModelBytes         = 1024
	maxRoutingEntries     = 4096
	routingMaxAge         = 15 * time.Minute
)

type routingDecision struct {
	backend string
	at      time.Time
}

type routingClient struct {
	endpoint *url.URL
	client   *http.Client
	now      func() time.Time
	mu       sync.Mutex
	lastGood map[string]routingDecision
}

// Deadlines cover request writes and response body reads, not just headers.
type routingConn struct{ net.Conn }

func (c routingConn) Read(b []byte) (int, error) {
	if err := c.SetReadDeadline(time.Now().Add(routingReadTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Read(b)
}

func (c routingConn) Write(b []byte) (int, error) {
	if err := c.SetWriteDeadline(time.Now().Add(routingSendTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(b)
}

func newRoutingClient(endpoint *url.URL) *routingClient {
	dialer := &net.Dialer{Timeout: routingConnectTimeout}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true
	// Fresh HTTP/1 connections avoid implicit stale-connection or HTTP/2 stream retries.
	transport.DisableKeepAlives = true
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return routingConn{conn}, nil
	}
	transport.TLSHandshakeTimeout = routingConnectTimeout
	transport.ResponseHeaderTimeout = routingReadTimeout
	transport.MaxResponseHeaderBytes = 8 << 10
	return &routingClient{
		endpoint: endpoint,
		client: &http.Client{
			Transport:     transport,
			Timeout:       routingConnectTimeout + routingSendTimeout + routingReadTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		now: time.Now, lastGood: make(map[string]routingDecision),
	}
}

// Match routing_selector.lua, including generation 0 only for unavailable.
func parseRoutingDecision(body []byte) string {
	var result map[string]json.RawMessage
	if len(body) > maxRoutingBody || json.Unmarshal(body, &result) != nil {
		return ""
	}
	var backend *string
	var gen *float64
	if json.Unmarshal(result["decision"], &backend) != nil || json.Unmarshal(result["generation"], &gen) != nil || backend == nil || gen == nil {
		return ""
	}
	generation := *gen
	if generation < 0 || math.Trunc(generation) != generation {
		return ""
	}
	decision := *backend
	switch decision {
	case "cpa", "aisix", "not_found", "unavailable":
	default:
		return ""
	}
	if generation == 0 && decision != "unavailable" {
		return ""
	}
	return decision
}

func (r *routingClient) lookup(ctx context.Context, model string) string {
	target := *r.endpoint
	query := target.Query()
	query.Set("model", model)
	target.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return ""
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.ContentLength > maxRoutingBody {
		return ""
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRoutingBody+1))
	if err != nil {
		return ""
	}
	return parseRoutingDecision(body)
}

func (r *routingClient) decide(ctx context.Context, model string) (string, *apiError) {
	decision := r.lookup(ctx, model)
	if decision == "" && ctx.Err() != nil {
		return "", routingUnavailable()
	}
	now := r.now()
	r.mu.Lock()
	switch decision {
	case "cpa", "aisix":
		if _, exists := r.lastGood[model]; !exists && len(r.lastGood) >= maxRoutingEntries {
			var oldest string
			var at time.Time
			for key, value := range r.lastGood {
				if at.IsZero() || value.at.Before(at) {
					oldest, at = key, value.at
				}
			}
			delete(r.lastGood, oldest)
		}
		r.lastGood[model] = routingDecision{decision, now}
	case "not_found", "unavailable":
		delete(r.lastGood, model)
	default:
		if stored, ok := r.lastGood[model]; ok {
			age := now.Sub(stored.at)
			if age >= 0 && age < routingMaxAge {
				decision = stored.backend
				r.mu.Unlock()
				log.Printf("WARN routing-index fallback model=%q age=%s", model, age.Round(time.Second))
				return decision, nil
			}
			delete(r.lastGood, model)
		}
	}
	r.mu.Unlock()
	if ctx.Err() != nil {
		return "", routingUnavailable()
	}
	switch decision {
	case "cpa", "aisix":
		return decision, nil
	case "not_found":
		return "", &apiError{status: 404, typ: "invalid_request_error", code: "model_not_found", message: "Model not found."}
	default:
		return "", routingUnavailable()
	}
}

func routingUnavailable() *apiError {
	return &apiError{status: 503, typ: "server_error", code: "service_unavailable", message: "Model routing is temporarily unavailable."}
}

func invalidRoutingRequest() *apiError {
	return &apiError{status: 400, typ: "invalid_request_error", code: "invalid_request_error", message: "A valid top-level model is required."}
}

// Skip nested values token by token; never retain the whole input array for routing.
func skipJSONValue(dec *json.Decoder) (json.Token, error) {
	first, err := dec.Token()
	if err != nil {
		return nil, err
	}
	if first != json.Delim('{') && first != json.Delim('[') {
		return first, nil
	}
	for depth := 1; depth > 0; {
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		switch token {
		case json.Delim('{'), json.Delim('['):
			depth++
			if depth > 10000 {
				return nil, errWorkSize
			}
		case json.Delim('}'), json.Delim(']'):
			depth--
		}
	}
	return first, nil
}

func routingMetadata(raw *spool, ws bool) (string, bool, *apiError) {
	dec := json.NewDecoder(io.NewSectionReader(raw.File, raw.offset, raw.size))
	dec.UseNumber()
	start, err := dec.Token()
	if err != nil || start != json.Delim('{') {
		return "", false, invalidRoutingRequest()
	}
	var model string
	var stream bool
	for fields := 0; dec.More(); fields++ {
		if fields >= 256 {
			return "", stream, workingError(errWorkSize)
		}
		token, err := dec.Token()
		if err != nil {
			return "", stream, invalidRoutingRequest()
		}
		key, ok := token.(string)
		if !ok || len(key) > 64<<10 {
			return "", stream, workingError(errWorkSize)
		}
		value, err := skipJSONValue(dec)
		if err != nil {
			return "", stream, invalidRoutingRequest()
		}
		switch key {
		case "model":
			model, _ = value.(string)
		case "stream":
			stream, _ = value.(bool)
		case "type":
			if ws && value != "response.create" {
				return "", stream, &apiError{status: 400, typ: "invalid_request_error", param: "type", message: "unsupported event type"}
			}
		}
	}
	if _, err := dec.Token(); err != nil {
		return "", stream, invalidRoutingRequest()
	}
	if _, err := dec.Token(); err != io.EOF || model == "" || len(model) > maxModelBytes {
		return "", stream, invalidRoutingRequest()
	}
	return model, stream, nil
}

func (k *keeper) routeRequest(ctx context.Context, raw *spool, ws bool) (*url.URL, bool, *apiError) {
	if k.routing == nil {
		return k.cfg.upstream, false, nil
	}
	leave, err := k.work.enter(ctx)
	if err != nil {
		return nil, false, workingError(err)
	}
	model, stream, apiErr := routingMetadata(raw, ws)
	leave()
	if apiErr != nil {
		return nil, stream, apiErr
	}
	// No materialization phase or history lock spans a network lookup.
	backend, apiErr := k.routing.decide(ctx, model)
	if apiErr != nil {
		return nil, stream, apiErr
	}
	if backend == "cpa" {
		return k.cfg.upstreamCPA, stream, nil
	}
	return k.cfg.upstreamAISIX, stream, nil
}

func writeRoutingError(w http.ResponseWriter, failure *apiError, stream bool) {
	if !stream {
		writeAPIError(w, failure)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(failure.status)
	payload, _ := json.Marshal(map[string]any{"type": "error", "status": failure.status, "error": failure.object()})
	_, _ = fmt.Fprintf(w, "event: error\ndata: %s\n\n", payload)
}

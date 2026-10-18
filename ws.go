package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// serveWS runs the Responses WebSocket mode: each response.create frame is
// expanded like an HTTP request, sent upstream as streaming HTTP, and every
// upstream SSE event is relayed back as one text frame. Turns run in order.
func (k *keeper) serveWS(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	conn.SetReadLimit(k.cfg.maxRequestBytes)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	header := r.Header.Clone()
	header.Del("Upgrade")
	owner := ownerOf(r)

	frames := make(chan []byte)
	go func() {
		defer cancel() // a closed or broken client socket aborts the in-flight turn
		defer close(frames)
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if typ != websocket.MessageText {
				continue
			}
			select {
			case frames <- data:
			case <-ctx.Done():
				return
			}
		}
	}()

	// The most recent turn stays reachable on this socket even with store:false.
	var lastID string
	var lastItems []json.RawMessage
	for data := range frames {
		start := time.Now()
		id, items, status, hasPrev := k.wsTurn(ctx, conn, header, owner, data, lastID, lastItems)
		if id != "" {
			lastID, lastItems = id, items
		}
		logTurn("ws", status, hasPrev, id, start, k.store)
		if ctx.Err() != nil {
			break
		}
	}
	_ = conn.Close(websocket.StatusNormalClosure, "")
}

func (k *keeper) wsTurn(ctx context.Context, conn *websocket.Conn, header http.Header, owner [32]byte, data []byte, lastID string, lastItems []json.RawMessage) (string, []json.RawMessage, int, bool) {
	var doc map[string]json.RawMessage
	if json.Unmarshal(data, &doc) != nil || doc == nil {
		return "", nil, wsError(ctx, conn, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", message: "frame must be a JSON object"}), false
	}
	var kind string
	_ = json.Unmarshal(doc["type"], &kind)
	if kind != "response.create" {
		return "", nil, wsError(ctx, conn, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", param: "type", message: "unsupported event type"}), false
	}
	delete(doc, "type")
	doc["stream"] = json.RawMessage("true")
	hasPrev := doc["previous_response_id"] != nil
	t, apiErr := prepare(doc, func(id string) ([]json.RawMessage, bool) {
		if id == lastID && lastItems != nil {
			return lastItems, true
		}
		return k.store.get(id, owner)
	})
	if apiErr != nil {
		return "", nil, wsError(ctx, conn, apiErr), hasPrev
	}
	resp, err := k.forward(ctx, "/v1/responses", header, t.body)
	if err != nil {
		return "", nil, wsError(ctx, conn, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"}), hasPrev
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		writeUpstreamError(ctx, conn, resp.StatusCode, body)
		return "", nil, resp.StatusCode, hasPrev
	}
	var c collector
	err = readSSE(resp.Body, nil, func(event []byte) error {
		c.event(event)
		return conn.Write(ctx, websocket.MessageText, rewriteResponse(event, t.store))
	})
	if err != nil {
		log.Printf("ws relay ended early: %v", err)
	}
	rid, output, ok := c.final()
	if !ok {
		return "", nil, resp.StatusCode, hasPrev
	}
	id := publicID(rid)
	items := append(t.input, output...)
	if t.store {
		k.store.put(id, owner, items)
	}
	return id, items, resp.StatusCode, hasPrev
}

func wsError(ctx context.Context, conn *websocket.Conn, e *apiError) int {
	frame, _ := json.Marshal(map[string]any{"type": "error", "status": e.status, "error": e.object()})
	_ = conn.Write(ctx, websocket.MessageText, frame)
	return e.status
}

// writeUpstreamError relays an upstream failure verbatim inside an error frame,
// keeping the upstream status and error object.
func writeUpstreamError(ctx context.Context, conn *websocket.Conn, status int, body []byte) {
	var parsed struct {
		Error json.RawMessage `json:"error"`
	}
	errObj := json.RawMessage(nil)
	if json.Unmarshal(body, &parsed) == nil && len(parsed.Error) > 0 && parsed.Error[0] == '{' {
		errObj = parsed.Error
	}
	if errObj == nil {
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = http.StatusText(status)
		}
		errObj, _ = json.Marshal(map[string]any{"type": "upstream_error", "message": message, "code": nil, "param": nil})
	}
	frame, err := json.Marshal(map[string]any{"type": "error", "status": status, "error": errObj})
	if err != nil {
		frame = []byte(`{"type":"error","error":{"type":"upstream_error","message":"upstream request failed"}}`)
	}
	if werr := conn.Write(ctx, websocket.MessageText, frame); werr != nil && !errors.Is(werr, context.Canceled) {
		log.Printf("ws error frame write failed: %v", werr)
	}
}

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRegressionCROnlySSE(t *testing.T) {
	n := 0
	err := readSSE(strings.NewReader("data: {\"type\":\"x\"}\r\r"), nil, func([]byte) error { n++; return nil })
	if n != 1 || err != nil {
		t.Fatalf("valid CR-only SSE: dispatches=%d err=%v; want 1,nil", n, err)
	}
}

func TestRegressionPrewarmContinuation(t *testing.T) {
	up, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	id := responseID(wsCreate(t, c, `{"type":"response.create","model":"m","generate":false,"input":[]}`))
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"next","previous_response_id":"`+id+`"}`)
	if ev["type"] != "response.completed" {
		t.Fatalf("prewarm id %q cannot continue: %+v; downstream frame=%s", id, ev, mustRaw(t, up, 1))
	}
}

func TestRegressionStoreFalseSameSocket(t *testing.T) {
	_, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	id := responseID(wsCreate(t, c, `{"type":"response.create","model":"m","input":"A","store":false}`))
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"B","previous_response_id":"`+id+`"}`)
	if ev["type"] != "response.completed" {
		t.Fatalf("store:false same-socket continuation id=%q fails: %+v", id, ev)
	}
}

// Sub2API WS passthrough forces store:false on every response.create
// (openai_gateway_request_body.go normalizeOpenAIResponsesWebSocketCompatibilityBody).
// IGNORE_CLIENT_STORE_FALSE must apply on the native WS relay exactly as on
// HTTP: the turn registers in the shared store, advertises store:true, and
// continues on a fresh socket and over HTTP. Codex clients send store:false
// themselves, so the flag is required wherever the keeper terminates WS.
func TestRegressionWSNativeIgnoreClientStoreFalse(t *testing.T) {
	up, srv, k := newWSNativeKeeper(t, func(c *config) { c.ignoreClientStoreFalse = true })
	c1 := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c1, `{"type":"response.create","model":"m","input":"A","store":false}`)
	if resp, _ := ev["response"].(map[string]any); resp["store"] != true {
		t.Fatalf("ignored store:false must advertise store:true: %v", ev)
	}
	id := responseID(ev)
	if !strings.HasPrefix(id, "resp_") {
		t.Fatalf("id not rewritten for client: %v", ev)
	}
	if n, _ := k.store.stats(); n != 1 {
		t.Fatalf("shared store entries = %d, want 1", n)
	}
	c1.Close(websocket.StatusNormalClosure, "")

	// New socket continues from the registered turn.
	c2 := dialWS(t, srv, "Bearer a")
	ev = wsCreate(t, c2, `{"type":"response.create","model":"m","input":"B","store":false,"previous_response_id":"`+id+`"}`)
	if ev["type"] != "response.completed" {
		t.Fatalf("new-socket continuation = %v", ev)
	}
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B" {
		t.Fatalf("new-socket input = %s", got)
	}
	c2.Close(websocket.StatusNormalClosure, "")

	// HTTP continues from the same turn; the stateless upstream rejects any
	// forwarded previous_response_id, so a completed response proves
	// expansion happened locally.
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"C","stream":true,"previous_response_id":"`+id+`"}`)
	if status != http.StatusOK {
		t.Fatalf("http continuation %d %s", status, body)
	}
	if completedID(t, body) == "" {
		t.Fatalf("http continuation had no completed id: %s", body)
	}
}

// Without the flag the store:false turn stays pinned to its socket: a new
// socket sees previous_response_not_found.
func TestRegressionWSNativeStoreFalsePinWithoutFlag(t *testing.T) {
	_, srv, k := newWSNativeKeeper(t)
	c1 := dialWS(t, srv, "Bearer a")
	id := responseID(wsCreate(t, c1, `{"type":"response.create","model":"m","input":"A","store":false}`))
	c1.Close(websocket.StatusNormalClosure, "")
	if n, _ := k.store.stats(); n != 0 {
		t.Fatalf("unflagged store:false must not register: entries=%d", n)
	}
	c2 := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c2, `{"type":"response.create","model":"m","input":"B","previous_response_id":"`+id+`"}`)
	errObj, _ := ev["error"].(map[string]any)
	if ev["type"] != "error" || errObj["code"] != "previous_response_not_found" {
		t.Fatalf("unflagged store:false id must not continue cross-socket: %v", ev)
	}
}

func TestRegressionDisconnectCleanup(t *testing.T) {
	seen := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		if _, _, e = c.Read(r.Context()); e != nil {
			return
		}
		close(seen)
		_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","response":{"id":"probe_id","status":"in_progress","output":[]}}`))
		for {
			if _, _, e = c.Read(r.Context()); e != nil {
				return
			}
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"A"}`)); e != nil {
		t.Fatal(e)
	}
	<-seen
	if _, _, e := c.Read(ctx); e != nil {
		t.Fatal(e)
	}
	_ = c.CloseNow()
	time.Sleep(200 * time.Millisecond)
	k.work.mu.Lock()
	disk, output := k.work.disk, k.work.output
	k.work.mu.Unlock()
	paths, _ := filepath.Glob(spoolFilePattern(k.cfg.storeDir))
	if disk != 0 || output != 0 || len(paths) != 0 {
		t.Fatalf("after client disconnect: disk=%d output=%d spoolFiles=%d; want all zero", disk, output, len(paths))
	}
}

func TestRegressionInvalidUTF8Rejected(t *testing.T) {
	_, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if e := c.Write(ctx, websocket.MessageText, []byte{0xff}); e != nil {
		t.Fatal(e)
	}
	_, raw, e := c.Read(ctx)
	if websocket.CloseStatus(e) != websocket.StatusInvalidFramePayloadData {
		t.Fatalf("invalid UTF-8: payload=%x err=%v status=%d; want close 1007", raw, e, websocket.CloseStatus(e))
	}
}

func TestRegressionDownstreamClosePropagated(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		_ = c.Close(websocket.StatusGoingAway, "maintenance")
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, _, e := c.Read(ctx)
	if websocket.CloseStatus(e) != websocket.StatusGoingAway {
		t.Fatalf("downstream normal close lost: err=%v status=%d; want 1001", e, websocket.CloseStatus(e))
	}
}

func TestRegressionWriterErrorNoPanic(t *testing.T) {
	_, srv, k := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	_ = c.CloseNow()
	raw, e := k.spoolRequest(context.Background(), strings.NewReader(`{"type":"response.create","model":"m","input":"A"}`))
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	var inflight *wsNativeInflight
	var native bool
	defer func() {
		if x := recover(); x != nil {
			t.Fatalf("upstream Writer error panicked: %v", x)
		}
	}()
	_, _, _ = k.wsNativeClientFrame(context.Background(), c, c, httptest.NewRequest("GET", "/v1/responses", nil), [32]byte{}, &wsFrame{typ: websocket.MessageText, spool: raw}, &wsSession{}, &inflight, &native)
}

func TestRegressionUnknownEventUnchanged(t *testing.T) {
	_, _, k := newWSNativeKeeper(t)
	raw := []byte(`{"type":"vendor.telemetry","id":"trace_7","sequence_number":42,"payload":{"x":1}}`)
	var c collector
	defer c.release(k.work)
	wire, release, e := k.responseFrame(context.Background(), &c, raw, true, true, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if string(wire) != string(raw) {
		t.Fatalf("unknown event changed: %s", wire)
	}
}

func TestRegressionNon101BodyUntruncated(t *testing.T) {
	body := strings.Repeat("x", 2048)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(403); _, _ = io.WriteString(w, body) }))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	req := httptest.NewRequest("GET", "/v1/responses", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	rec := httptest.NewRecorder()
	k.ServeHTTP(rec, req)
	if rec.Code != 403 || rec.Body.String() != body {
		t.Fatalf("handshake rejection changed: status=%d body_len=%d; want 403,2048", rec.Code, rec.Body.Len())
	}
}

func TestRegressionPromptCacheRetentionPreserved(t *testing.T) {
	var forwarded map[string]json.RawMessage
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &forwarded)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"resp_probe","object":"response","status":"completed","store":false,"output":[]}`)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	status, _, _ := post(t, srv, "Bearer a", `{"model":"m","input":"A","prompt_cache_retention":"24h"}`)
	if status != 200 {
		t.Fatalf("status %d", status)
	}
	if string(forwarded["prompt_cache_retention"]) != `"24h"` {
		t.Fatalf("cache tool dropped upstream prompt_cache_retention: %s", forwarded["prompt_cache_retention"])
	}
}

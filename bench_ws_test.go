package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// benchUpstream answers response.create frames instantly with a minimal
// completed sequence, so the benchmark measures the keeper's relay path.
type benchUpstream struct{ n atomic.Int64 }

func (b *benchUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(64 << 20)
	defer conn.CloseNow()
	for {
		typ, raw, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			continue
		}
		var doc struct {
			Type string `json:"type"`
			Prev string `json:"previous_response_id"`
		}
		_ = json.Unmarshal(raw, &doc)
		if doc.Type != "response.create" {
			continue
		}
		if doc.Prev != "" {
			// Stateless upstream: reject any parent it did not serve.
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"previous_response_not_found"}}`))
			continue
		}
		n := b.n.Add(1)
		id := fmt.Sprintf("bench_%d", n)
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","response":{"id":"`+id+`","status":"in_progress","output":[]}}`))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.completed","response":{"id":"`+id+`","status":"completed","store":false,"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"ok"}]}]}}`))
	}
}

func benchKeeper(b *testing.B) (*keeper, *httptest.Server) {
	up := &benchUpstream{}
	upSrv := httptest.NewServer(up)
	b.Cleanup(upSrv.Close)
	u, _ := url.Parse(upSrv.URL)
	cfg := config{upstream: u, ttl: time.Minute, maxStoreBytes: 64 << 20, maxRequestBytes: 16 << 20,
		storeDir: b.TempDir(), storeMetaBytes: 16 << 20, storePinBytes: 8 << 20, storeCompactBytes: 128 << 20}
	k := newKeeper(cfg)
	srv := httptest.NewServer(k)
	b.Cleanup(srv.Close)
	b.Cleanup(func() { _ = k.store.close() })
	return k, srv
}

func dialBenchWS(b *testing.B, srv *httptest.Server) *websocket.Conn {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, strings.Replace(srv.URL, "http", "ws", 1)+"/v1/responses", nil)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { c.CloseNow() })
	return c
}

// wsTurn runs one create frame and drains events until response.completed.
func wsTurn(b *testing.B, c *websocket.Conn, frame string) {
	ctx := context.Background()
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		b.Fatal(err)
	}
	for {
		_, raw, err := c.Read(ctx)
		if err != nil {
			b.Fatal(err)
		}
		var ev struct {
			Type string `json:"type"`
		}
		_ = json.Unmarshal(raw, &ev)
		if ev.Type == "response.completed" || ev.Type == "response.failed" || ev.Type == "error" {
			return
		}
	}
}

// BenchmarkWSMissPassthrough measures a response.create with an unknown
// previous_response_id: the frame relays byte-for-byte, unrecorded, and the
// upstream's own previous_response_not_found error relays back.
func BenchmarkWSMissPassthrough(b *testing.B) {
	_, srv := benchKeeper(b)
	c := dialBenchWS(b, srv)
	frame := `{"type":"response.create","model":"m","input":"x","previous_response_id":"native_upstream_1"}`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wsTurn(b, c, frame)
	}
}

// BenchmarkWSRecordTurn measures a no-parent turn: forward + per-event relay +
// incremental record.
func BenchmarkWSRecordTurn(b *testing.B) {
	_, srv := benchKeeper(b)
	c := dialBenchWS(b, srv)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wsTurn(b, c, `{"type":"response.create","model":"m","input":"x"}`)
	}
}

// BenchmarkWSExpandHit measures expansion of a cached parent with a large
// stored history streamed from disk.
func BenchmarkWSExpandHit(b *testing.B) {
	_, srv := benchKeeper(b)
	c := dialBenchWS(b, srv)
	_ = c
	b.Skip("superseded by BenchmarkWSExpandHitStore")
}

// BenchmarkWSExpandHitStore expands a ~2 MiB stored history, written straight
// into the store the bench keeper uses.
func BenchmarkWSExpandHitStore(b *testing.B) {
	k, srv := benchKeeper(b)
	items := make([]json.RawMessage, 200)
	for i := range items {
		items[i] = json.RawMessage(fmt.Sprintf(`{"type":"message","role":"user","content":[{"type":"input_text","text":%q}]}`, strings.Repeat("x", 10<<10)))
	}
	if _, err := k.store.put("resp_big", [32]byte{}, "", items); err != nil {
		b.Fatal(err)
	}
	c := dialBenchWS(b, srv)
	frame := `{"type":"response.create","model":"m","input":"next","previous_response_id":"resp_big"}`
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wsTurn(b, c, frame)
	}
}

// BenchmarkHTTPMissPassthrough measures an HTTP POST whose parent is unknown:
// spool + native reverse-proxy relay, unrecorded.
func BenchmarkHTTPMissPassthrough(b *testing.B) {
	_, srv := benchKeeper(b)
	_ = srv
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		resp, err := http.Post(srv.URL+"/v1/responses", "application/json",
			strings.NewReader(`{"model":"m","input":"x","previous_response_id":"native_upstream_1"}`))
		if err != nil {
			b.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

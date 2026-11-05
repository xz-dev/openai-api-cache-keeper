package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// postPath posts body to path; chunked hides the length (Transfer-Encoding:
// chunked) so keeper must count the bytes as they arrive.
func postPath(t *testing.T, srv *httptest.Server, method, path, auth, body string, chunked bool) (int, string) {
	t.Helper()
	var rd io.Reader = strings.NewReader(body)
	if chunked {
		rd = io.MultiReader(rd) // no Len method: the client sends it chunked
	}
	req, _ := http.NewRequest(method, srv.URL+path, rd)
	req.Header.Set("Authorization", auth)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

func dialWSPath(t *testing.T, srv *httptest.Server, path, auth string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+path, &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {auth}}})
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

// Major 3 (1): /backend-api/codex/responses is covered like /v1/responses.
// A 64 MiB body is admitted, ticketed and spooled, and reaches the upstream
// on its own path with the ticket, so no hop below answers 503
// capacity_ticket_missing.
func TestCodexResponsesPathTicketed(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	status, out := postPath(t, srv, http.MethodPost, "/backend-api/codex/responses", "Bearer big", largeBody(int(largeBodyBytes)), false)
	if status != 200 || !strings.Contains(out, "response.completed") {
		t.Fatalf("codex POST %d %.300s", status, out)
	}
	gu.mu.Lock()
	hdrs, paths := append([]string(nil), gu.headers...), append([]string(nil), gu.paths...)
	gu.mu.Unlock()
	if len(hdrs) != 1 || len(hdrs[0]) != 32 || len(paths) != 1 || paths[0] != "/backend-api/codex/responses" {
		t.Fatalf("upstream tickets %q paths %q", hdrs, paths)
	}
	k.work.mu.Lock()
	peak := k.work.diskPeak
	k.work.mu.Unlock()
	if peak < largeBodyBytes {
		t.Fatalf("spool peak %d: the 64 MiB body was not spooled", peak)
	}
	waitLedgerEmpty(t, k)

	// The WS form of the path is covered too: the rebuilt frame carries the ticket first.
	c := dialWSPath(t, srv, "/backend-api/codex/responses", "Bearer big")
	ctx, cancel := context.WithTimeout(context.Background(), upstreamFrameWait)
	defer cancel()
	frame := `{"type":"response.create",` + largeBody(int(largeBodyBytes))[1:]
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatalf("client write of the 64 MiB codex frame (limit %v): %v", upstreamFrameWait, err)
	}
	tails := waitUpstreamFrames(t, gu, 1, "the 64 MiB codex WS frame")
	if !strings.HasPrefix(tails[0], `{"ai_gateway_capacity_ticket":"`) {
		t.Fatalf("codex WS frame head: %q", tails[0])
	}
	gu.mu.Lock()
	paths = append([]string(nil), gu.paths...)
	gu.mu.Unlock()
	if paths[len(paths)-1] != "/backend-api/codex/responses" {
		t.Fatalf("codex WS upstream path %q", paths)
	}
	c.Close(websocket.StatusNormalClosure, "")
	waitLedgerEmpty(t, k)
}

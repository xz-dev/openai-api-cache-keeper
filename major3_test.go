package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
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

func fallbackBodyOf(n int) string {
	head, tail := `{"contents":"`, `"}`
	return head + strings.Repeat("x", n-len(head)-len(tail)) + tail
}

func upstreamPOSTs(gu *ticketUpstream) []string {
	gu.mu.Lock()
	defer gu.mu.Unlock()
	return append([]string(nil), gu.paths...)
}

// Major 3 (2, 4): a fallback path above L gets the named 413 before any
// upstream call, ticket or spool byte. The shape is Sub2API's
// unconverted-route 413, with keeper as the hop.
func TestFallbackPathRefusedAboveL(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	var admissions atomic.Int64 // every ticket admission reads the memory ledger
	k.gate.memAvailable = func() (int64, error) { admissions.Add(1); return 901 * mib, nil }
	status, out := postPath(t, srv, http.MethodPost, "/v1/chat/completions", "Bearer big", fallbackBodyOf(70<<20), false)
	want := `{"error":{"code":"request_too_large","hop":"keeper","message":"keeper: /v1/chat/completions is a fallback path; its limit is 16777216 bytes and the body has 73400320","param":null,"type":"invalid_request_error"}}`
	if status != 413 || strings.TrimSpace(out) != want {
		t.Fatalf("70 MiB chat: %d %s", status, out)
	}
	k.work.mu.Lock()
	peak := k.work.diskPeak
	k.work.mu.Unlock()
	gu.mu.Lock()
	billing := gu.billing
	gu.mu.Unlock()
	if p := upstreamPOSTs(gu); len(p) != 0 || peak != 0 || admissions.Load() != 0 || billing != 0 {
		t.Fatalf("70 MiB chat reached upstream %q, spooled %d bytes, %d ticket admissions, %d billing calls", p, peak, admissions.Load(), billing)
	}
	waitLedgerEmpty(t, k)

	// Just under and just over L on the Gemini and Antigravity routes, with
	// a declared length and counted as a chunked body.
	for _, path := range []string{"/v1beta/models/g:generateContent", "/antigravity/v1/messages"} {
		for _, chunked := range []bool{false, true} {
			before := len(upstreamPOSTs(gu))
			if status, out := postPath(t, srv, http.MethodPost, path, "Bearer big", fallbackBodyOf(int(fallbackLimit)), chunked); status == 413 {
				t.Fatalf("%s at exactly L (chunked %t): %d %.300s", path, chunked, status, out)
			}
			if got := len(upstreamPOSTs(gu)); got != before+1 {
				t.Fatalf("%s at exactly L (chunked %t) did not reach the upstream", path, chunked)
			}
			status, out := postPath(t, srv, http.MethodPost, path, "Bearer big", fallbackBodyOf(int(fallbackLimit)+1), chunked)
			msg := "keeper: " + path + " is a fallback path; its limit is 16777216 bytes"
			if !chunked {
				msg += " and the body has 16777217"
				if got := len(upstreamPOSTs(gu)); got != before+1 {
					t.Fatalf("%s at L+1 with a length reached the upstream", path)
				}
			}
			if status != 413 || !strings.Contains(out, `"message":"`+msg+`"`) || !strings.Contains(out, `"hop":"keeper"`) {
				t.Fatalf("%s at L+1 (chunked %t): %d %.300s", path, chunked, status, out)
			}
		}
	}

	// Admin and management routes keep today's behaviour: no keeper limit.
	before := len(upstreamPOSTs(gu))
	if status, out := postPath(t, srv, http.MethodPost, "/api/v1/admin/accounts/data", "Bearer big", fallbackBodyOf(int(fallbackLimit)+1), false); status == 413 {
		t.Fatalf("admin import refused: %.300s", out)
	}
	if got := len(upstreamPOSTs(gu)); got != before+1 {
		t.Fatal("admin import did not reach the upstream")
	}

	// A Responses body of 64 MiB still goes through auth and the gate.
	status, out = postPath(t, srv, http.MethodPost, "/v1/responses", "Bearer big", largeBody(int(largeBodyBytes)), false)
	gu.mu.Lock()
	last := gu.headers[len(gu.headers)-1]
	gu.mu.Unlock()
	if status != 200 || len(last) != 32 || admissions.Load() == 0 {
		t.Fatalf("64 MiB Responses: %d ticket %q %.200s", status, last, out)
	}
	waitLedgerEmpty(t, k)
}

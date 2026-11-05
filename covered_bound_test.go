package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// boundUpstream records what reaches the upstream: body bytes per request,
// billing calls and upgrades.
type boundUpstream struct {
	mu       sync.Mutex
	requests []string // "METHOD path bytes"
	bytes    int64
	billing  int
	active   atomic.Int64 // upstream handlers still running
}

func boundKeeper(t *testing.T) (*httptest.Server, *boundUpstream) {
	t.Helper()
	bu := &boundUpstream{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		bu.active.Add(1)
		defer bu.active.Add(-1)
		if r.URL.Path == "/v1/sub2api/billing" {
			bu.mu.Lock()
			bu.billing++
			bu.mu.Unlock()
			return
		}
		if isUpgradeRequest(r) {
			n, _ := io.Copy(io.Discard, r.Body)
			bu.mu.Lock()
			bu.requests = append(bu.requests, r.Method+" "+r.URL.Path+" upgrade")
			bu.bytes += n
			bu.mu.Unlock()
			if c, err := websocket.Accept(w, r, nil); err == nil {
				c.CloseNow()
			}
			return
		}
		n, _ := io.Copy(io.Discard, r.Body)
		bu.mu.Lock()
		bu.requests = append(bu.requests, r.Method+" "+r.URL.Path)
		bu.bytes += n
		bu.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	return srv, bu
}

// snapshot waits until no upstream handler runs (an aborted body is still
// being counted after keeper answered), then reads the counters.
func (bu *boundUpstream) snapshot(t *testing.T) (int, int64, int) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for bu.active.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("upstream handlers still running after 1 min")
		}
		time.Sleep(time.Millisecond)
	}
	bu.mu.Lock()
	defer bu.mu.Unlock()
	return len(bu.requests), bu.bytes, bu.billing
}

// sendBound sends method to path with an optional WebSocket Upgrade and a
// body of size bytes (size < 0: no body), declared or chunked.
// A transport error is returned, not fatal: a gated handshake that keeper
// answers while the body is still being written can reset the write.
func sendBound(t *testing.T, srv *httptest.Server, method, path string, upgrade bool, size int, chunked bool) (int, string, error) {
	t.Helper()
	var body io.Reader
	if size >= 0 {
		body = strings.NewReader(fallbackBodyOf(size))
		if chunked {
			body = io.MultiReader(body)
		}
	}
	req, _ := http.NewRequest(method, srv.URL+path, body)
	req.Header.Set("Authorization", "Bearer b")
	if upgrade {
		req.Header.Set("Connection", "Upgrade")
		req.Header.Set("Upgrade", "websocket")
		req.Header.Set("Sec-WebSocket-Version", "13")
		req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusSwitchingProtocols {
		return resp.StatusCode, "", nil
	}
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	return resp.StatusCode, string(raw), nil
}

// Review 2026-11-05 M1: on a covered path only POST without Upgrade and GET
// with Upgrade are gated. Every other method × Upgrade combination is
// bounded by L with the named 413, or refused when it cannot be bounded (a
// non-GET Upgrade), and never reaches the upstream unbounded.
func TestCoveredPathUngatedRequestsBounded(t *testing.T) {
	srv, bu := boundKeeper(t)
	L := int(fallbackLimit)
	type body struct {
		size    int
		chunked bool
	}
	bodies := []body{{-1, false}, {L, false}, {L + 1, false}, {L + 1, true}}
	methods := []string{http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete, http.MethodOptions}
	for _, path := range []string{"/v1/responses", "/responses", "/backend-api/codex/responses", "/v1/responses/sub"} {
		for _, method := range methods {
			for _, upgrade := range []bool{false, true} {
				gated := coveredPath(path) && ((method == http.MethodPost && !upgrade) || (method == http.MethodGet && upgrade))
				for _, b := range bodies {
					if path != "/v1/responses" && b.size == L {
						continue // exactly L is checked on one path; the rest only bound
					}
					name := method + " " + path
					if upgrade {
						name += " +Upgrade"
					}
					reqs0, bytes0, bill0 := bu.snapshot(t)
					status, out, err := sendBound(t, srv, method, path, upgrade, b.size, b.chunked)
					if err != nil && !(gated && upgrade) {
						t.Fatalf("%s size=%d chunked=%t: %v", name, b.size, b.chunked, err)
					}
					reqs, bytes, bill := bu.snapshot(t)
					sent := bytes - bytes0
					notGated := strings.Contains(out, "is not a gated Responses request")
					switch {
					case gated:
						if notGated {
							t.Fatalf("%s size=%d: gated request got the L refusal: %d %.300s", name, b.size, status, out)
						}
						if upgrade && sent != 0 {
							t.Fatalf("%s size=%d: the handshake body reached the upstream (%d bytes)", name, b.size, sent)
						}
					case upgrade && method != http.MethodGet:
						if status != 400 || !strings.Contains(out, `"code":"upgrade_not_bounded"`) || !strings.Contains(out, `"hop":"keeper"`) || reqs != reqs0 || bill != bill0 {
							t.Fatalf("%s size=%d chunked=%t: %d %.300s; upstream requests +%d billing +%d", name, b.size, b.chunked, status, out, reqs-reqs0, bill-bill0)
						}
					case b.size > L:
						if status != 413 || !strings.Contains(out, `"code":"request_too_large"`) || !strings.Contains(out, `"hop":"keeper"`) || !strings.Contains(out, `"limit_bytes":16777216`) {
							t.Fatalf("%s size=%d chunked=%t: %d %.300s", name, b.size, b.chunked, status, out)
						}
						if coveredPath(path) && !notGated {
							t.Fatalf("%s: 413 does not name the ungated request: %.300s", name, out)
						}
						if sent > int64(L) || bill != bill0 || (!b.chunked && reqs != reqs0) {
							t.Fatalf("%s size=%d chunked=%t: %d bytes upstream, requests +%d, billing +%d", name, b.size, b.chunked, sent, reqs-reqs0, bill-bill0)
						}
					default:
						if status == 413 || status == 400 {
							t.Fatalf("%s size=%d within L refused: %d %.300s", name, b.size, status, out)
						}
						if b.size == L && sent != int64(L) {
							t.Fatalf("%s at exactly L: %d bytes upstream", name, sent)
						}
					}
				}
			}
		}
	}
}

package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/sys/unix"
)

// authKeeper fronts fakeUpstream with a billing endpoint answering status.
// spooled reports whether any spool byte was written.
func authKeeper(t *testing.T, status int) (*httptest.Server, *keeper, *atomic.Int64, *atomic.Bool) {
	var billing atomic.Int64
	fake := &fakeUpstream{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sub2api/billing" {
			billing.Add(1)
			w.WriteHeader(status)
			return
		}
		if isUpgradeRequest(r) {
			c, err := websocket.Accept(w, r, nil)
			if err == nil {
				defer c.CloseNow()
				_, _, _ = c.Read(r.Context())
			}
			return
		}
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u; c.maxRequestBytes = 96 << 20 })
	var spooled atomic.Bool
	k.work.statfs = func(fd int, st *unix.Statfs_t) error { spooled.Store(true); return unix.Fstatfs(fd, st) }
	return srv, k, &billing, &spooled
}

func largeBody(n int) string {
	head, tail := `{"model":"m","stream":true,"store":false,"input":"`, `"}`
	return head + strings.Repeat("x", n-len(head)-len(tail)) + tail
}

func postBody(t *testing.T, srv *httptest.Server, body io.Reader, length int64) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", body)
	req.ContentLength = length
	req.Header.Set("Authorization", "Bearer big")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestLargeBodyAuthenticatedFirst(t *testing.T) {
	body := largeBody(int(largeBodyBytes))
	for _, tc := range []struct {
		name    string
		billing int
		chunked bool
		status  int
		code    string
	}{
		{"declared-401", 401, false, 401, "invalid_api_key"},
		{"declared-unavailable", 500, false, 503, "auth_unavailable"},
		{"chunked-401", 401, true, 401, "invalid_api_key"},
		{"declared-ok", 200, false, 200, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, k, billing, spooled := authKeeper(t, tc.billing)
			length := int64(len(body))
			var r io.Reader = strings.NewReader(body)
			if tc.chunked {
				length, r = -1, struct{ io.Reader }{r}
			}
			status, out := postBody(t, srv, r, length)
			if status != tc.status || !strings.Contains(out, tc.code) || billing.Load() != 1 {
				t.Fatalf("status=%d billing=%d body=%.300s", status, billing.Load(), out)
			}
			// A declared large body from a refused caller writes no spool.
			if !tc.chunked && tc.status != 200 && spooled.Load() {
				t.Fatal("refused caller's body was spooled")
			}
			waitWorking(t, k, 0)
		})
	}
}

// Bodies under 64 MiB keep today's path: no billing call by keeper.
func TestSmallBodySkipsLargeAuth(t *testing.T) {
	srv, _, billing, _ := authKeeper(t, 401)
	body := largeBody(int(largeBodyBytes) - 4096) // rebuilt upstream body stays under 64 MiB too
	if status, out := postBody(t, srv, struct{ io.Reader }{strings.NewReader(body)}, -1); status != 200 || billing.Load() != 0 {
		t.Fatalf("status=%d billing=%d %.200s", status, billing.Load(), out)
	}
}

// A WS message reaching 64 MiB from a refused caller gets a 401 error event.
func TestLargeWSMessageAuthenticatedFirst(t *testing.T) {
	srv, k, billing, _ := authKeeper(t, 401)
	c := dialWS(t, srv, "Bearer big")
	c.SetReadLimit(1 << 20)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame := `{"type":"response.create",` + largeBody(int(largeBodyBytes))[1:]
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	_, b, err := c.Read(ctx)
	if err != nil || !strings.Contains(string(b), `"status":401`) || !strings.Contains(string(b), "invalid_api_key") || billing.Load() != 1 {
		t.Fatalf("event=%.300s err=%v billing=%d", b, err, billing.Load())
	}
	c.Close(websocket.StatusNormalClosure, "")
	waitWorking(t, k, 0)
}

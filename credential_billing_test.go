package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

// Local task operations revalidate through Sub2API's billing endpoint on every
// call and fail closed. Sub2API rejections (inactive/disabled key, other
// group, IP ACL) keep their status and body; simple mode's 404 is not
// authentication; an unreachable validator does not expose the task; none of
// these cancel the task or start a generation. Plain traffic does not depend
// on the validator.
func TestBillingRevalidationFailsClosed(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"simple-mode", 404, `404 page not found`},
		{"key-disabled", 401, `{"code":"API_KEY_DISABLED","message":"API key is disabled"}`},
		{"group-mismatch", 403, `{"code":"GROUP_NOT_ALLOWED","message":"group"}`},
		{"ip-denied", 403, `{"code":"ACCESS_DENIED","message":"Access denied"}`},
		{"rate-limited", 429, `{"code":"RATE_LIMITED"}`},
		{"validator-error", 500, `{"code":"INTERNAL"}`},
		{"unreachable", 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var creates, plain atomic.Int32
			var reject atomic.Bool
			hold := make(chan struct{})
			defer close(hold)
			var billingURL *url.URL
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/sub2api/billing" {
					if reject.Load() && tc.status != 0 {
						w.Header().Set("X-Sub2api-Reason", tc.name)
						w.WriteHeader(tc.status)
						_, _ = io.WriteString(w, tc.body)
						return
					}
					_, _ = io.WriteString(w, `{}`)
					return
				}
				body, _ := io.ReadAll(r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				if !strings.Contains(string(body), `"background"`) && (strings.Contains(string(body), `"input":"B"`) || strings.Contains(string(body), `"content":"B"`)) {
					plain.Add(1)
					_, _ = io.WriteString(w, gateEvent("response.created", ""))
					_, _ = io.WriteString(w, gateEvent("response.completed", ""))
					return
				}
				creates.Add(1)
				_, _ = io.WriteString(w, gateEvent("response.created", ""))
				_, _ = io.WriteString(w, gateEvent("response.output_text.delta", "private"))
				w.(http.Flusher).Flush()
				<-hold
			}))
			t.Cleanup(up.Close)
			billingURL, _ = url.Parse(up.URL)
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = billingURL })
			resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
			resp.Body.Close()

			reject.Store(true)
			if tc.status == 0 {
				// Point local validation at a closed port; generation already runs.
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				dead := *billingURL
				dead.Host = ln.Addr().String()
				ln.Close()
				k.cfg.upstream = &dead
			}
			for _, op := range []struct{ method, suffix string }{
				{http.MethodGet, ""}, {http.MethodGet, "?stream=true&starting_after=0"}, {http.MethodPost, "/cancel"},
			} {
				req, _ := http.NewRequest(op.method, srv.URL+"/v1/responses/"+id+op.suffix, nil)
				req.Header.Set("Authorization", "Bearer owner")
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				body, _ := io.ReadAll(res.Body)
				res.Body.Close()
				if strings.Contains(string(body), "private") || res.StatusCode < 400 {
					t.Fatalf("%s %s not fail-closed: %d %s", op.method, op.suffix, res.StatusCode, body)
				}
				if tc.status != 0 && (res.StatusCode != tc.status || string(body) != tc.body || res.Header.Get("X-Sub2api-Reason") != tc.name) {
					t.Fatalf("%s %s upstream rejection altered: %d %q %v", op.method, op.suffix, res.StatusCode, body, res.Header)
				}
				if tc.status == 0 && res.StatusCode != http.StatusServiceUnavailable {
					t.Fatalf("%s %s unreachable validator: %d %s", op.method, op.suffix, res.StatusCode, body)
				}
			}
			k.cacheMu.Lock()
			task := k.tasks[id]
			k.cacheMu.Unlock()
			if task == nil {
				t.Fatal("rejected operation dropped the task")
			}
			task.mu.Lock()
			cancelled := task.cancelled
			task.mu.Unlock()
			if cancelled || creates.Load() != 1 {
				t.Fatalf("rejected operation changed task: cancelled=%t generations=%d", cancelled, creates.Load())
			}
			if tc.status != 0 {
				// Ordinary traffic is not gated by the cache validator.
				status, body, _ := post(t, srv, "Bearer owner", `{"model":"m","input":"B","stream":true,"store":false}`)
				if status != http.StatusOK || plain.Load() != 1 || !strings.Contains(body, "response.completed") {
					t.Fatalf("plain traffic gated by validator: %d plain=%d %s", status, plain.Load(), body)
				}
			}
		})
	}
}

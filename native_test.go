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
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// echoUpstream accepts previous_response_id and records every raw request
// body verbatim, so transparency assertions compare exact bytes.
type echoUpstream struct {
	mu     sync.Mutex
	bodies []string
	n      int
}

func (e *echoUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	raw, _ := io.ReadAll(r.Body)
	e.mu.Lock()
	e.bodies = append(e.bodies, string(raw))
	e.n++
	n := e.n
	e.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, `{"id":"native_%d","object":"response","status":"completed","output":[]}`, n)
}

func (e *echoUpstream) body(i int) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.bodies[i]
}

func (e *echoUpstream) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.n
}

func newEchoKeeper(t *testing.T) (*echoUpstream, *httptest.Server, *keeper) {
	t.Helper()
	up := &echoUpstream{}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)
	u, _ := url.Parse(upSrv.URL)
	up2, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	_ = up2
	return up, srv, k
}

// (1) A canonical POST with a native previous_response_id and unknown fields
// reaches the upstream byte-for-byte unchanged.
func TestNativePostRawBodyUnknownFieldsAndPrevID(t *testing.T) {
	up, srv, _ := newEchoKeeper(t)
	const body = `{"model":"native","input":"B","previous_response_id":"native_1","future_field":{"nested":[1,2,null]},"weird":"<&>\u00e9"}`
	status, got, _ := post(t, srv, "Bearer native", body)
	if status != 200 || !strings.Contains(got, `"native_1"`) {
		t.Fatalf("status=%d body=%s", status, got)
	}
	if up.body(0) != body {
		t.Fatalf("upstream body changed:\n got: %s\nwant: %s", up.body(0), body)
	}
}

// (2) A locally owned checkpoint id continues on the canonical path with the
// full parent history expanded upstream.
func TestLocalCheckpointContinuesOnCanonicalPath(t *testing.T) {
	up, srv, k := newEchoKeeper(t)
	auth := "Bearer local"
	req, _ := http.NewRequest("POST", srv.URL+"/v1/responses", nil)
	req.Header.Set("Authorization", auth)
	owner := ownerOf(req)
	if _, err := k.store.put("resp_seed", owner, "", rawItems(`"P1"`, `"P2"`)); err != nil {
		t.Fatal(err)
	}
	status, got, _ := post(t, srv, auth, `{"model":"native","input":"B","previous_response_id":"resp_seed"}`)
	if status != 200 || !strings.Contains(got, `"status":"completed"`) {
		t.Fatalf("continuation %d %s", status, got)
	}
	want := `{"input":["P1","P2",{"content":"B","role":"user"}],"model":"native"}`
	if up.body(0) != want {
		t.Fatalf("expanded history wrong:\n got: %s\nwant: %s", up.body(0), want)
	}
}

// (3) A keeper-owned id that is expired, or belongs to another owner, fails
// closed with exactly one error and zero upstream creations.
func TestForeignAndExpiredLocalIDsFailClosed(t *testing.T) {
	up, srv, k := newEchoKeeper(t)
	req, _ := http.NewRequest("POST", srv.URL+"/v1/responses", nil)
	ownerA := ownerOf(withAuth(req, "Bearer a"))
	if _, err := k.store.put("resp_seed", ownerA, "", rawItems(`"P1"`)); err != nil {
		t.Fatal(err)
	}
	// Foreign owner: known local id, wrong caller.
	status, body, _ := post(t, srv, "Bearer b", `{"model":"native","input":"X","previous_response_id":"resp_seed"}`)
	if status != 400 || strings.Count(body, "previous_response_not_found") != 1 || strings.Count(body, `"error"`) != 1 {
		t.Fatalf("foreign: %d %s", status, body)
	}
	// Expired for the right owner.
	k.store.mu.Lock()
	k.store.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	k.store.mu.Unlock()
	status, body, _ = post(t, srv, "Bearer a", `{"model":"native","input":"X","previous_response_id":"resp_seed"}`)
	if status != 400 || strings.Count(body, "previous_response_not_found") != 1 {
		t.Fatalf("expired: %d %s", status, body)
	}
	if up.count() != 0 {
		t.Fatalf("fail-closed id reached upstream %d times", up.count())
	}
}

// (4) A native upstream id stays native: the second turn is forwarded with the
// reference intact and only the new input, and the keeper registers nothing.
func TestNativeUpstreamIDRemainsNative(t *testing.T) {
	up, srv, k := newEchoKeeper(t)
	if _, body, _ := post(t, srv, "Bearer native", `{"model":"native","input":"A"}`); !strings.Contains(body, `"native_1"`) {
		t.Fatalf("turn1: %s", body)
	}
	status, body, _ := post(t, srv, "Bearer native", `{"model":"native","input":"B","previous_response_id":"native_1"}`)
	if status != 200 || !strings.Contains(body, `"native_2"`) {
		t.Fatalf("turn2: %d %s", status, body)
	}
	if got := up.body(1); got != `{"model":"native","input":"B","previous_response_id":"native_1"}` {
		t.Fatalf("native turn not incremental: %s", got)
	}
	if n, _ := k.store.stats(); n != 0 {
		t.Fatalf("native ids must not enter the local registry: %d entries", n)
	}
}

// (5) A native WebSocket upgrade refused by the upstream keeps the upstream
// status, headers, and body.
func TestNativeWSNon101Preserved(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Upstream-Probe", "kept")
		w.WriteHeader(http.StatusForbidden)
		io.WriteString(w, `{"error":{"code":"no_ws_for_you"}}`)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	ctx, cancel := timeoutCtx(5 * time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/responses", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Bearer native"}}})
	if err == nil {
		t.Fatal("dial unexpectedly succeeded")
	}
	if resp == nil || resp.StatusCode != 403 || resp.Header.Get("X-Upstream-Probe") != "kept" {
		t.Fatalf("non-101 not preserved: %v %+v", err, resp)
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "no_ws_for_you") {
		t.Fatalf("upstream body lost: %s", body)
	}
}

func rawItems(items ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(items))
	for i, s := range items {
		out[i] = json.RawMessage(s)
	}
	return out
}

func withAuth(r *http.Request, auth string) *http.Request {
	r.Header.Set("Authorization", auth)
	return r
}

func timeoutCtx(d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithDeadline(context.Background(), time.Now().Add(d))
}

func TestNativeUnrelatedTraffic(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("X-Received-URI", r.URL.RequestURI())
		w.Header().Set("X-Received-Method", r.Method)
		w.Header().Set("X-Received-Authorization", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusTeapot)
		_, _ = w.Write(body)
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	for _, tc := range []struct{ method, path string }{
		{"POST", "/rk/v1/responses?unknown=one%2Btwo"},
		{"PATCH", "/internal/unknown?a=1&a=2"},
		{"DELETE", "/internal/item"},
		{"GET", "/v1/responses/resp_native?stream=true&starting_after=17"},
		{"POST", "/v1/responses/resp_native/cancel"},
		{"OPTIONS", "/internal/unknown"},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			t.Parallel()
			const body = `{"unknown":[1,null,"raw"]}`
			req, err := http.NewRequest(tc.method, srv.URL+tc.path, strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			req.Header.Set("Authorization", "Bearer native")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			got, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != http.StatusTeapot || string(got) != body ||
				resp.Header.Get("X-Received-URI") != tc.path ||
				resp.Header.Get("X-Received-Method") != tc.method ||
				resp.Header.Get("X-Received-Authorization") != "Bearer native" {
				t.Fatalf("status=%d headers=%v body=%s", resp.StatusCode, resp.Header, got)
			}
		})
	}
}

func TestNativeWSRawMessagesRemainNative(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.CloseNow()
		for {
			kind, body, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if err := conn.Write(r.Context(), kind, body); err != nil {
				return
			}
		}
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	conn := dialWS(t, srv, "Bearer native")
	for _, message := range []struct {
		kind websocket.MessageType
		body []byte
	}{
		{websocket.MessageText, []byte(`{"type":"response.create","previous_response_id":"resp_native","input":"delta","unknown":{"raw":true}}`)},
		{websocket.MessageBinary, []byte{0, 1, 2, 255}},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := conn.Write(ctx, message.kind, message.body); err != nil {
			cancel()
			t.Fatal(err)
		}
		kind, body, err := conn.Read(ctx)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if kind != message.kind || string(body) != string(message.body) {
			t.Fatalf("message changed: kind=%d body=%q", kind, body)
		}
	}
}

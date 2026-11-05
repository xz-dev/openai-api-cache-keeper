package main

import (
	"context"
	"encoding/json"
	"errors"
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

// tunnelKeeper fronts an echo WS upstream on every non-Responses path; the
// upstream records the size of each whole message it received.
func tunnelKeeper(t *testing.T) (*httptest.Server, func() []int) {
	var mu sync.Mutex
	var got []int
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer a" {
			http.Error(w, `{"error":"no key"}`, http.StatusUnauthorized)
			return
		}
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		c.SetReadLimit(-1)
		for {
			typ, b, err := c.Read(context.Background())
			if err != nil {
				return
			}
			mu.Lock()
			got = append(got, len(b))
			mu.Unlock()
			if err := c.Write(context.Background(), typ, b); err != nil {
				return
			}
		}
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	return srv, func() []int { mu.Lock(); defer mu.Unlock(); return append([]int(nil), got...) }
}

// Major 3 (3): a native non-Responses WS message is capped at L. A message
// of exactly L is relayed; one byte more, whole or fragmented, gets the
// bridge's named error event and a normal close, and never reaches the
// upstream whole.
func TestNativeTunnelMessageCappedAtL(t *testing.T) {
	srv, received := tunnelKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c := dialWSPath(t, srv, "/v1/realtime?model=m", "Bearer a")
	c.SetReadLimit(-1)
	under := strings.Repeat("u", int(fallbackLimit))
	if err := c.Write(ctx, websocket.MessageText, []byte(under)); err != nil {
		t.Fatalf("write L: %v", err)
	}
	pinged := make(chan error, 1)
	go func() { pinged <- c.Ping(ctx) }() // control frames pass both ways; Read takes the pong
	if _, b, err := c.Read(ctx); err != nil || len(b) != len(under) {
		t.Fatalf("echo of L: %d bytes, %v", len(b), err)
	}
	// A small echo after the ping: its Read handles the pong that precedes it.
	if err := c.Write(ctx, websocket.MessageText, []byte("x")); err != nil {
		t.Fatalf("write x: %v", err)
	}
	if _, b, err := c.Read(ctx); err != nil || string(b) != "x" {
		t.Fatalf("echo of x: %q %v", b, err)
	}
	if err := <-pinged; err != nil {
		t.Fatalf("ping through the tunnel: %v", err)
	}
	for _, fragmented := range []bool{false, true} {
		if fragmented {
			c = dialWSPath(t, srv, "/v1/realtime", "Bearer a")
		}
		over := []byte(strings.Repeat("o", int(fallbackLimit)+1))
		var err error
		if fragmented {
			wr, _ := c.Writer(ctx, websocket.MessageText)
			for i := 0; i < len(over) && err == nil; i += 1 << 20 {
				_, err = wr.Write(over[i:min(i+1<<20, len(over))])
			}
			if err == nil {
				err = wr.Close()
			}
		} else {
			err = c.Write(ctx, websocket.MessageText, over)
		}
		_ = err // the tunnel may close before the client finishes writing
		_, b, err := c.Read(ctx)
		if err != nil {
			t.Fatalf("fragmented %t: no error event before the close: %v", fragmented, err)
		}
		var ev struct {
			Type   string         `json:"type"`
			Status int            `json:"status"`
			Error  map[string]any `json:"error"`
		}
		if json.Unmarshal(b, &ev) != nil || ev.Type != "error" || ev.Status != 413 || ev.Error["code"] != "request_too_large" ||
			ev.Error["hop"] != "keeper" || ev.Error["limit"] != float64(fallbackLimit) || ev.Error["type"] != "invalid_request_error" {
			t.Fatalf("fragmented %t: error event %s", fragmented, b)
		}
		_, _, err = c.Read(ctx)
		var ce websocket.CloseError
		if !errors.As(err, &ce) || ce.Code != websocket.StatusNormalClosure || ce.Reason != "request too large" {
			t.Fatalf("fragmented %t: close %v", fragmented, err)
		}
	}
	if got := received(); len(got) != 2 || got[0] != int(fallbackLimit) || got[1] != 1 {
		t.Fatalf("upstream received messages of %v bytes, want only [%d 1]", got, fallbackLimit)
	}
}

// The handshake is relayed as before: a refused upgrade reaches the client
// with its status and body.
func TestNativeTunnelRelaysRefusedHandshake(t *testing.T) {
	srv, _ := tunnelKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/realtime", nil)
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("refused upgrade: %v %v", resp, err)
	}
	if b, _ := io.ReadAll(resp.Body); !strings.Contains(string(b), "no key") {
		t.Fatalf("refused upgrade body %q", b)
	}
}

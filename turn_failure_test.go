package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type lockedLog struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// The virtio incident shape: the inner hop closes the socket with 1009 while
// a turn is open. The client must get an explicit error event naming the
// limit class, never only a bare close, and the log names side and code.
func TestInnerCloseDuringTurnIsExplicit(t *testing.T) {
	for _, tc := range []struct {
		name    string
		code    websocket.StatusCode
		status  float64
		errCode string
	}{
		{"1009", websocket.StatusMessageTooBig, 413, "request_too_large"},
		{"1011", websocket.StatusInternalError, 502, "upstream_closed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &lockedLog{}
			old := log.Writer()
			log.SetOutput(logs)
			defer log.SetOutput(old)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				if _, _, err = c.Read(r.Context()); err != nil {
					return
				}
				_ = c.Close(tc.code, "message too big")
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
			c := dialWS(t, srv, "Bearer a")
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"hi"}`)); err != nil {
				t.Fatal(err)
			}
			_, b, err := c.Read(ctx)
			if err != nil {
				t.Fatalf("bare close: client got no error event before close: %v", err)
			}
			var ev struct {
				Type   string  `json:"type"`
				Status float64 `json:"status"`
				Error  struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if json.Unmarshal(b, &ev) != nil || ev.Type != "error" || ev.Status != tc.status || ev.Error.Code != tc.errCode ||
				!strings.Contains(ev.Error.Message, "keeper") {
				t.Fatalf("error event = %s", b)
			}
			if _, _, err = c.Read(ctx); websocket.CloseStatus(err) != tc.code {
				t.Fatalf("close after error event: %v, want %d", err, tc.code)
			}
			deadline := time.Now().Add(2 * time.Second)
			for !strings.Contains(logs.String(), "ws turn ended early: side=upstream") && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
			line := logs.String()
			if !strings.Contains(line, "ws turn ended early: side=upstream close_code="+itoa(int(tc.code))) ||
				!strings.Contains(line, "request_id=") || !strings.Contains(line, "transport=ws status=502") {
				t.Fatalf("cause line missing: %s", line)
			}
		})
	}
}

func itoa(n int) string { b, _ := json.Marshal(n); return string(b) }

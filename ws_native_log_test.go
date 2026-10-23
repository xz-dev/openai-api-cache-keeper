package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type wsTurnLog chan string

func (lines wsTurnLog) Write(p []byte) (int, error) {
	if strings.Contains(string(p), "transport=ws ") {
		lines <- string(p)
	}
	return len(p), nil
}

func TestWSNativeLogsAtTurnEnd(t *testing.T) {
	for _, tc := range []struct {
		name, kind, parent string
		stored, abort      bool
		status             int
	}{
		{name: "completed", kind: "response.completed", stored: true, status: 200},
		{name: "incomplete", kind: "response.incomplete", stored: true, status: 200},
		{name: "failed", kind: "response.failed", status: 200},
		{name: "error", kind: "error", status: 200},
		{name: "native parent", kind: "error", parent: "resp_downstream", status: 200},
		{name: "abort", abort: true, status: 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			lines := make(wsTurnLog, 16)
			old := log.Writer()
			log.SetOutput(lines)
			defer log.SetOutput(old)
			ready, finish := make(chan struct{}), make(chan struct{})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer c.CloseNow()
				if _, _, err = c.Read(ctx); err != nil {
					return
				}
				if err = c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.created","response":{"id":"msg_log","status":"in_progress","output":[]}}`)); err != nil {
					return
				}
				close(ready)
				select {
				case <-finish:
				case <-ctx.Done():
					return
				}
				if !tc.abort {
					state := strings.TrimPrefix(tc.kind, "response.")
					_ = c.Write(ctx, websocket.MessageText, []byte(fmt.Sprintf(`{"type":%q,"response":{"id":"msg_log","status":%q,"output":[]}}`, tc.kind, state)))
				}
				_, _, _ = c.Read(ctx)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, _, k := newKeeperFixture(t, false, func(c *config) { c.upstream = u })
			done := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				k.ServeHTTP(w, r)
			}))
			defer srv.Close()
			c := dialWS(t, srv, "Bearer a")
			defer c.CloseNow()
			prev := ""
			if tc.parent != "" {
				prev = fmt.Sprintf(`,"previous_response_id":%q`, tc.parent)
			}
			if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"A"`+prev+`}`)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := c.Read(ctx); err != nil {
				t.Fatal(err)
			}
			<-ready
			select {
			case line := <-lines:
				t.Fatalf("turn logged before terminal: %s", line)
			default:
			}
			// A delayed terminal must be included in the logged duration.
			select {
			case <-time.After(25 * time.Millisecond):
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			close(finish)
			if !tc.abort {
				_, raw, err := c.Read(ctx)
				if err != nil {
					t.Fatal(err)
				}
				var ev map[string]any
				if err := json.Unmarshal(raw, &ev); err != nil {
					t.Fatal(err)
				}
				if tc.stored && responseID(ev) != "resp_msg_log" {
					t.Fatalf("public id: %v", ev)
				}
			}
			_ = c.CloseNow()
			select {
			case <-done:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			select {
			case line := <-lines:
				for _, field := range []string{fmt.Sprintf("status=%d", tc.status), fmt.Sprintf("previous=%t", tc.parent != ""), fmt.Sprintf("stored=%t", tc.stored)} {
					if !strings.Contains(line, field) {
						t.Fatalf("missing %s: %s", field, line)
					}
				}
				var duration string
				for _, field := range strings.Fields(line) {
					if strings.HasPrefix(field, "duration=") {
						duration = strings.TrimPrefix(field, "duration=")
					}
				}
				if d, err := time.ParseDuration(duration); err != nil || d < 25*time.Millisecond {
					t.Fatalf("duration excludes terminal wait: %s", line)
				}
			default:
				t.Fatal("missing completed turn log")
			}
			select {
			case line := <-lines:
				t.Fatalf("duplicate turn log: %s", line)
			default:
			}
		})
	}
}

func TestWSNativeLogsLocalRejectionNotOpaqueFrames(t *testing.T) {
	lines := make(wsTurnLog, 16)
	old := log.Writer()
	log.SetOutput(lines)
	defer log.SetOutput(old)
	_, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"opaque"}`)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := c.Read(ctx); err != nil {
		t.Fatal(err)
	}
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"A","previous_response_id":"`+keeperResponsePrefix+`missing"}`)
	if ev["type"] != "error" {
		t.Fatalf("expected local rejection: %v", ev)
	}
	select {
	case line := <-lines:
		if !strings.Contains(line, "status=400 previous=true stored=false") {
			t.Fatalf("wrong rejection log: %s", line)
		}
	case <-ctx.Done():
		t.Fatal("missing rejected turn log")
	}
	select {
	case line := <-lines:
		t.Fatalf("opaque frame or rejection logged twice: %s", line)
	default:
	}
}

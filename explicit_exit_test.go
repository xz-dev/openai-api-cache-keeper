package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func captureLog(t *testing.T) *lockedLog {
	logs := &lockedLog{}
	old := log.Writer()
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(old) })
	return logs
}

func waitLog(logs *lockedLog, want string) string {
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), want) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	return logs.String()
}

// An inner hop that drops the connection yields an explicit 502 naming
// keeper and the cause, and the 502 log line carries side, cause and id.
func TestHTTPInnerFailureIsExplicitAndLogged(t *testing.T) {
	logs := captureLog(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, _, err := http.NewResponseController(w).Hijack()
		if err == nil {
			c.Close()
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"model":"m","input":"hi"}`))
	req.Header.Set("Authorization", "Bearer a")
	req.Header.Set("X-Request-Id", "req-502")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("bare failure: %v", err)
	}
	defer resp.Body.Close()
	var body struct {
		Error struct{ Code, Message string } `json:"error"`
	}
	if resp.StatusCode != 502 || json.NewDecoder(resp.Body).Decode(&body) != nil || body.Error.Code != "upstream_failed" ||
		!strings.Contains(body.Error.Message, "keeper") {
		t.Fatalf("502 = %d %+v", resp.StatusCode, body)
	}
	line := waitLog(logs, "keeper 502:")
	if !strings.Contains(line, "keeper 502: side=upstream") || !strings.Contains(line, "request_id=req-502") || !strings.Contains(line, "cause=") {
		t.Fatalf("502 log line: %s", line)
	}
	waitWorking(t, k, 0)
}

// A WS upgrade whose inner dial fails gets an explicit JSON 502, logged.
func TestWSDialFailureIsExplicitAndLogged(t *testing.T) {
	logs := captureLog(t)
	dead := httptest.NewServer(http.NotFoundHandler())
	u, _ := url.Parse(dead.URL)
	dead.Close()
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/responses",
		&websocket.DialOptions{HTTPHeader: http.Header{"X-Request-Id": {"req-dial"}}})
	if err == nil || resp == nil || resp.StatusCode != 502 {
		t.Fatalf("dial: %v %v", err, resp)
	}
	if line := waitLog(logs, "where=ws dial"); !strings.Contains(line, "request_id=req-dial") {
		t.Fatalf("dial log: %s", line)
	}
}

// An upstream frame over the keeper limit ends the open turn with an event
// that names the limit; a socket closed with no open turn logs its cause.
func TestWSUpstreamOversizeFrameNamesLimit(t *testing.T) {
	logs := captureLog(t)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		if _, _, err = c.Read(r.Context()); err != nil {
			return
		}
		_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","x":"`+strings.Repeat("x", 8192)+`"}`))
		_, _, _ = c.Read(r.Context())
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u; c.maxRequestBytes = 4096 })
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	_, b, err := c.Read(ctx)
	if err != nil {
		t.Fatalf("bare close: %v", err)
	}
	if !strings.Contains(string(b), `"type":"error"`) || !strings.Contains(string(b), "4096-byte limit (MAX_REQUEST_BYTES)") {
		t.Fatalf("event = %s", b)
	}
	if line := waitLog(logs, "ws turn ended early: side=keeper"); !strings.Contains(line, "side=keeper") {
		t.Fatalf("log: %s", line)
	}

	idle := dialWS(t, srv, "Bearer b")
	_ = idle.Close(websocket.StatusNormalClosure, "bye")
	if line := waitLog(logs, "ws session closed: side=client close_code=1000"); !strings.Contains(line, "ws session closed: side=client close_code=1000") {
		t.Fatalf("session log: %s", line)
	}
}

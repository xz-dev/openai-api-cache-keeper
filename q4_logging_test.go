package main

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Q4: the first cause of a close is kept; later causes are appended.
func TestTurnEndKeepsFirstCause(t *testing.T) {
	first := turnEnd{side: "client", code: -1, cause: "write to client: broken pipe"}
	got := first.then(turnEnd{side: "upstream", code: 1006, cause: "close frame"})
	if got.side != "client" || got.code != -1 || !strings.HasPrefix(got.cause, "write to client: broken pipe; then side=upstream close_code=1006") {
		t.Fatalf("first cause lost: %+v", got)
	}
}

// Q4: a WS close line carries a keeper request id when the upgrade had
// none, the socket age and the time since the last frame from each side.
func TestWSCloseLogCarriesIDAndClock(t *testing.T) {
	logs := captureLog(t)
	_, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	if ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`); ev["type"] != "response.completed" {
		t.Fatalf("turn: %v", ev)
	}
	_ = c.Close(websocket.StatusNormalClosure, "bye")
	line := waitLog(logs, "ws session closed: side=client close_code=1000")
	re := regexp.MustCompile(`ws session closed: side=client close_code=1000 .* request_id=keeper-\S+ age=\S+ since_client_frame=\d\S* since_upstream_frame=\d\S*`)
	if !re.MatchString(line) {
		t.Fatalf("close line: %s", line)
	}
}

// Task 3.1 incident replay: a 1.55 MB WS continuation delta expands to
// about 17.9 MB of history; an inner hop limited to 16 MiB closes 1009.
// The client gets an explicit error event naming keeper, never a bare close.
func TestIncidentReplayInnerLimitErrorEvent(t *testing.T) {
	up, srv, _ := newWSNativeKeeper(t, func(c *config) {
		c.maxRequestBytes = 256 << 20
		c.maxStoreBytes, c.storePinBytes, c.storeMetaBytes, c.storeCompactBytes = 1<<30, 256<<20, 32<<20, 1<<30
	})
	up.readLimit.Store(16 << 20)
	status, out, _ := post(t, srv, "Bearer a", turnBody("", strings.Repeat("h", 16350000)))
	if status != 200 {
		t.Fatalf("turn1 %d %.200s", status, out)
	}
	id1 := completedID(t, out)
	c := dialWS(t, srv, "Bearer a")
	frame := `{"type":"response.create","model":"m","input":"` + strings.Repeat("d", 1550000) + `","previous_response_id":"` + id1 + `"}`
	ev := wsTerminal(t, c, frame)
	errObj, _ := ev["error"].(map[string]any)
	// The inner hop's 1009 becomes a named 413 event (code, reason, hop).
	if ev["type"] != "error" || errObj["code"] != "request_too_large" || !strings.Contains(fmt.Sprint(errObj["message"]), "keeper: the inner hop closed the WebSocket with 1009") {
		t.Fatalf("incident replay: %v", ev)
	}
	// With the inner hop at 256 MiB the same replay succeeds.
	up.readLimit.Store(256 << 20)
	c = dialWS(t, srv, "Bearer a")
	if ev := wsTerminal(t, c, frame); ev["type"] != "response.completed" {
		t.Fatalf("replay at 256 MiB: %v", ev)
	}
}

// M8: a WS continuation that ends in an inner 503 error event is logged with
// that status and code, not with the socket's 200; on a local (recorded)
// continuation and on a native-parent one.
func TestWSTurnLogCarriesErrorEventStatus(t *testing.T) {
	logs := captureLog(t)
	up, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`)
	if ev["type"] != "response.completed" {
		t.Fatalf("turn1: %v", ev)
	}
	id1 := ev["response"].(map[string]any)["id"].(string)
	up.errFrame.Store(`{"type":"error","status":503,"error":{"type":"server_error","code":"capacity_busy","message":"inner busy"}}`)
	const want = `transport=ws status=503 code="capacity_busy" previous=true`
	for i, prev := range []string{id1, "resp_native"} {
		ev = wsCreate(t, c, `{"type":"response.create","model":"m","input":"B","previous_response_id":"`+prev+`"}`)
		if ev["type"] != "error" {
			t.Fatalf("continuation %d: %v", i, ev)
		}
		deadline := time.Now().Add(time.Minute)
		for strings.Count(logs.String(), want) < i+1 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		if got := strings.Count(logs.String(), want); got != i+1 {
			t.Fatalf("continuation %d (previous %s): want %d turn lines %q, logs:\n%s", i, prev, i+1, want, logs.String())
		}
	}
	if strings.Contains(logs.String(), "transport=ws status=200 previous=true") {
		t.Fatalf("an error turn was logged as 200:\n%s", logs.String())
	}
}

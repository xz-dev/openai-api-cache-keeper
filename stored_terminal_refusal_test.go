package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// nearLimitAfterStore makes keeper's cgroup look full once a turn has been
// stored, so the terminal frame's output reservation is refused after the
// successful store (the 256 MiB acceptance bug).
func nearLimitAfterStore(k *keeper) {
	k.work.cgroup = func() (int64, int64, bool) {
		if n, _ := k.store.stats(); n > 0 {
			return 384 << 20, 384<<20 - cgroupMargin, true
		}
		return 384 << 20, 64 << 20, true
	}
}

// D9: HTTP SSE — a refusal after the store ends in an explicit error event,
// not a truncated stream.
func TestStoredTurnRefusedTerminalHTTPErrorEvent(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	nearLimitAfterStore(k)
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"hi","stream":true}`)
	if n, _ := k.store.stats(); status != 200 || n != 1 {
		t.Fatalf("status %d, stored %d: %s", status, n, body)
	}
	if strings.Contains(body, "response.completed") || !strings.Contains(body, "event: response.failed") {
		t.Fatalf("want response.failed, not completed or truncation:\n%s", body)
	}
	waitWorking(t, k, 0)
}

// D9: WS — the same refusal ends the turn with an explicit failure frame.
func TestStoredTurnRefusedTerminalWSErrorEvent(t *testing.T) {
	_, srv, k := newLocalTestKeeper(t)
	nearLimitAfterStore(k)
	conn := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("socket ended without a terminal: %v", err)
		}
		var ev struct{ Type string }
		_ = json.Unmarshal(data, &ev)
		if ev.Type == "response.completed" {
			t.Fatalf("completed relayed despite refusal: %s", data)
		}
		if ev.Type == "response.failed" || ev.Type == "error" {
			break
		}
	}
	if n, _ := k.store.stats(); n != 1 {
		t.Fatalf("stored %d, want 1", n)
	}
	conn.Close(websocket.StatusNormalClosure, "")
	waitWorking(t, k, 0)
}

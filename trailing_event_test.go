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
	"testing"
	"time"

	"github.com/coder/websocket"
)

// R1: after a terminal with large kept ids is suspended for its commit (the
// commit itself runs; a key this large is served uncached), a trailing
// upstream event must still be relayed, with exactly one terminal and
// balanced credits. Ids above ~360 KB used to make the trailer's collect
// recount the leased ids and fail with a false 413 after the terminal.
func TestTrailingEventAfterLargeIDCommit(t *testing.T) {
	id := "resp_" + strings.Repeat("a", 400<<10)
	sse := fmt.Sprintf("data: {\"type\":\"response.created\",\"response\":{\"id\":%q,\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n", id) +
		fmt.Sprintf("data: {\"type\":\"response.completed\",\"response\":{\"id\":%q,\"object\":\"response\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":\"x\"}]}}\n\n", id) +
		"data: {\"type\":\"vendor.trailer\",\"v\":1}\n\n"
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isUpgradeRequest(r) {
			// Socket upstream: the same three events per response.create.
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			conn.SetReadLimit(4 << 20)
			for {
				if _, _, err := conn.Read(r.Context()); err != nil {
					return
				}
				for _, ev := range strings.Split(strings.TrimSpace(sse), "\n\n") {
					if conn.Write(r.Context(), websocket.MessageText, []byte(strings.TrimPrefix(ev, "data: "))) != nil {
						return
					}
				}
			}
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, sse)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u; c.maxStoreBytes = 64 << 20 })
	kinds := func(stream string) []string {
		var out []string
		for _, line := range strings.Split(stream, "\n") {
			if i := strings.Index(line, `"type":"`); strings.HasPrefix(line, "data: ") && i >= 0 {
				rest := line[i+8:]
				out = append(out, rest[:strings.IndexByte(rest, '"')])
			}
		}
		return out
	}
	want := "[response.created response.completed vendor.trailer]"

	t.Run("http", func(t *testing.T) {
		status, got, _ := post(t, srv, "", `{"input":"x","store":true,"stream":true}`)
		if status != 200 || fmt.Sprint(kinds(got)) != want {
			t.Fatalf("HTTP: status=%d events=%v", status, kinds(got))
		}
	})
	// Local socket turns read the upstream SSE through the same collector;
	// native socket turns end at the terminal and relay the trailer raw.
	_, local, lk := newLocalTestKeeper(t, func(c *config) { c.upstream = u; c.maxStoreBytes = 64 << 20 })
	t.Run("ws-local", func(t *testing.T) { wsTrailingTurns(t, local, want) })
	t.Run("ws-native", func(t *testing.T) { wsTrailingTurns(t, srv, want) })
	for _, k := range []*keeper{k, lk} {
		deadline := time.Now().Add(2 * time.Second)
		for c := admissionSnapshot(k); c.output != 0 || c.input != 0; c = admissionSnapshot(k) {
			if time.Now().After(deadline) {
				t.Fatalf("credits leaked: %+v", c)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func wsTrailingTurns(t *testing.T, srv *httptest.Server, want string) {
	// The same stream over a socket turn.
	conn := dialWS(t, srv, "")
	conn.SetReadLimit(4 << 20)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var events []string
	for i, frame := range []string{`{"type":"response.create","input":"x","store":true}`, `{"type":"response.create","input":"y","store":true}`} {
		if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
			t.Fatal(err)
		}
		events = events[:0]
		for len(events) < 3 {
			_, data, err := conn.Read(ctx)
			if err != nil {
				t.Fatalf("WS turn %d after %v: %v", i, events, err)
			}
			var ev struct{ Type string }
			_ = json.Unmarshal(data, &ev)
			events = append(events, ev.Type)
		}
		if fmt.Sprint(events) != want {
			t.Fatalf("WS turn %d: events=%v", i, events)
		}
	}
	conn.Close(websocket.StatusNormalClosure, "")
}

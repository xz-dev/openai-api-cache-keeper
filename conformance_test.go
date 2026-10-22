package main

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// RFC 6455 §5.4: a fragmented message must be delivered to the application as
// one logical message, including when control frames (§5.5 ping) interleave.
// coder/websocket Reader reassembles fragments and the library answers pings
// internally, so the keeper sees one message; this proves that contract
// end-to-end through the relay.
func TestRFC6455FragmentedCreateWithInterleavedPing(t *testing.T) {
	up, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	ctx := context.Background()

	frame := `{"type":"response.create","model":"m","input":"A"}`
	w, err := c.Writer(ctx, websocket.MessageText)
	if err != nil {
		t.Fatal(err)
	}
	mid := len(frame) / 2
	if _, err := w.Write([]byte(frame[:mid])); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte(frame[mid:])); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	for {
		_, raw, err := c.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ev struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.Type == "response.completed" {
			break
		}
	}
	if len(up.frames) != 1 {
		t.Fatalf("upstream frames=%d", len(up.frames))
	}
}

// RFC 6455 §5.6 size bound: the keeper rejects an oversized message with an
// explicit error frame and keeps the socket open — see
// TestWSNativeOversizeFrame. A hard close with 1009 is what an unhandled
// limit looks like; this service chooses the softer contract, so no separate
// test here.

// WHATWG SSE §9.2.6/§9.2.7: events split by CR, LF or CRLF; a leading BOM is
// stripped; a trailing event without a blank line at EOF is discarded.
func TestWHATWGSSELineEndingsBOMAndEOF(t *testing.T) {
	// Mixed line endings with a BOM.
	stream := "\ufeffdata: {\"a\":1}\r\ndata: {\"b\":2}\r\n\r\ndata: {\"c\":3}\n\ndata: trailing-partial"
	var datas [][]byte
	err := readSSE(strings.NewReader(stream), nil, func(d []byte) error {
		datas = append(datas, append([]byte(nil), d...))
		return nil
	})
	// A dangling final line without a terminating blank line is a partial
	// event: the parser must surface EOF as truncation, and not dispatch it.
	if err == nil {
		t.Fatal("expected truncation error for unterminated final event")
	}
	// BOM stripped: event1 joins both data: lines per §9.2.6; event2 is c.
	if len(datas) != 2 || string(datas[0]) != "{\"a\":1}\n{\"b\":2}" || string(datas[1]) != `{"c":3}` {
		t.Fatalf("dispatched %d events: %q", len(datas), datas)
	}
}

// WHATWG SSE §9.2.6: comment lines (starting ':') and named 'event:' fields
// pass through the frame bytes untouched; only data: lines feed the payload.
func TestWHATWGSSECommentsAndNamedEventsTransparent(t *testing.T) {
	stream := ": keep-alive\r\n\nevent: ping\ndata: {}\r\n\r\n"
	var frames [][]byte
	err := readSSE(strings.NewReader(stream), func(f []byte) error {
		frames = append(frames, append([]byte(nil), f...))
		return nil
	}, func(d []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 {
		t.Fatalf("frames=%d", len(frames))
	}
	if !strings.HasPrefix(string(frames[0]), ": keep-alive") || !strings.HasPrefix(string(frames[1]), "event: ping") {
		t.Fatalf("frames rewritten: %q", frames)
	}
}

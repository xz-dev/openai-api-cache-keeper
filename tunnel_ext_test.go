package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// deflateTunnelKeeper fronts a WS upstream that accepts permessage-deflate;
// it records the extension offer it saw and each whole message size.
func deflateTunnelKeeper(t *testing.T) (*httptest.Server, func() ([]string, []int)) {
	var mu sync.Mutex
	var offers []string
	var got []int
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		offers = append(offers, r.Header.Get("Sec-WebSocket-Extensions"))
		mu.Unlock()
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionContextTakeover})
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
			if err := c.Write(context.Background(), typ, b[:min(len(b), 16)]); err != nil {
				return
			}
		}
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	return srv, func() ([]string, []int) {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), offers...), append([]int(nil), got...)
	}
}

// Re-review 4: keeper does not negotiate permessage-deflate on a native
// tunnel, so L counts real message bytes: a compressible message of L+1
// bytes is refused although it would compress to a few KiB.
func TestNativeTunnelNoDeflateLCountsRealBytes(t *testing.T) {
	srv, seen := deflateTunnelKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	c, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/realtime", &websocket.DialOptions{CompressionMode: websocket.CompressionContextTakeover})
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	if ext := resp.Header.Get("Sec-WebSocket-Extensions"); ext != "" {
		t.Fatalf("client handshake negotiated %q", ext)
	}
	c.SetReadLimit(-1)
	_ = c.Write(ctx, websocket.MessageText, []byte(strings.Repeat("z", int(fallbackLimit)+1)))
	_, b, err := c.Read(ctx)
	var ev struct {
		Type  string         `json:"type"`
		Error map[string]any `json:"error"`
	}
	if err != nil || json.Unmarshal(b, &ev) != nil || ev.Type != "error" || ev.Error["code"] != "request_too_large" {
		t.Fatalf("compressible L+1 message: %.300s %v", b, err)
	}
	offers, got := seen()
	if len(offers) != 1 || offers[0] != "" || len(got) != 0 {
		t.Fatalf("upstream extension offers %q, messages %v", offers, got)
	}
}

// Re-review 4: a client control frame over 125 bytes fails the connection
// with 1002 (RFC 6455 §5.5); it is not relayed.
func TestNativeTunnelRefusesLargeControlFrame(t *testing.T) {
	srv, _ := tunnelKeeper(t)
	conn, err := net.Dial("tcp", strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(time.Minute))
	_, _ = io.WriteString(conn, "GET /v1/realtime HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer a\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n"+
		"Sec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n")
	br := bufio.NewReader(conn)
	hresp, err := http.ReadResponse(br, nil)
	if err != nil || hresp.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("handshake: %v %v", hresp, err)
	}
	payload := make([]byte, 126)
	frame := append([]byte{0x89, 0x80 | 126}, binary.BigEndian.AppendUint16(nil, 126)...)
	frame = append(append(frame, 0, 0, 0, 0), payload...) // zero mask: payload unchanged
	if _, err := conn.Write(frame); err != nil {
		t.Fatal(err)
	}
	var h wsFrameHead
	for {
		if err := readWSFrameHead(br, &h); err != nil {
			t.Fatalf("no close frame: %v", err)
		}
		body := make([]byte, h.n)
		if _, err := io.ReadFull(br, body); err != nil {
			t.Fatal(err)
		}
		if h.op == 0x8 {
			// keeper's own close, not the upstream failing a relayed frame
			if len(body) < 2 || binary.BigEndian.Uint16(body) != 1002 || string(body[2:]) != "control frame over 125 bytes or fragmented" {
				t.Fatalf("close payload %q", body)
			}
			return
		}
		if h.op == 0xA {
			t.Fatal("the 126-byte ping was relayed and answered")
		}
	}
}

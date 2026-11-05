package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

// serveNativeTunnel relays a native WebSocket on a non-Responses path (for
// example /v1/realtime) frame by frame instead of as an opaque byte tunnel,
// so each client message can be capped at L (design D2, review M3). Frames
// are forwarded unchanged and never buffered: the cap is decided from each
// frame header before its payload moves. A client message above L gets the
// bridge's named request_too_large error event and a normal close, never a
// bare close. The handshake itself (status, headers, non-101 answers) is
// relayed as the native proxy relays it.
func (k *keeper) serveNativeTunnel(w http.ResponseWriter, r *http.Request) {
	target := *k.cfg.upstream
	target.Path = strings.TrimRight(target.Path, "/") + r.URL.Path
	target.RawQuery = r.URL.RawQuery
	out := r.Clone(r.Context())
	out.URL, out.Host, out.RequestURI = &target, k.cfg.upstream.Host, ""
	out.Header = http.Header{}
	copyHeader(out.Header, r.Header) // drops hop-by-hop headers, including Upgrade
	out.Header.Set("Connection", "Upgrade")
	out.Header.Set("Upgrade", r.Header.Get("Upgrade"))
	resp, err := k.client.Transport.RoundTrip(out)
	if err != nil {
		var over *fallbackOverLimit
		if errors.As(err, &over) { // a counted handshake body crossed L
			writeAPIError(w, fallbackTooLarge(over.what, -1))
			return
		}
		writeAPIError(w, upstreamFailed(requestIDOf(r), "native tunnel", err))
		return
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		defer resp.Body.Close()
		copyHeader(w.Header(), resp.Header)
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
		return
	}
	back, ok := resp.Body.(io.ReadWriteCloser)
	if !ok {
		resp.Body.Close()
		writeAPIError(w, upstreamFailed(requestIDOf(r), "native tunnel", errors.New("101 without a connection")))
		return
	}
	defer back.Close()
	client, brw, err := http.NewResponseController(w).Hijack()
	if err != nil {
		writeAPIError(w, upstreamFailed(requestIDOf(r), "native tunnel", err))
		return
	}
	defer client.Close()
	resp.Body = nil
	if err := resp.Write(brw); err != nil || brw.Flush() != nil {
		return
	}
	t := &wsTunnel{client: client, back: back}
	done := make(chan struct{})
	go func() {
		defer close(done)
		t.down(bufio.NewReader(back))
		if !t.refusing.Load() { // refuse still writes the event and closes
			client.Close()
		}
	}()
	t.up(brw.Reader, requestIDOf(r), r.URL.Path)
	back.Close()
	<-done
}

// wsTunnel is one relayed native socket. mu orders writes to the client:
// the upstream's frames and keeper's own error event never interleave.
type wsTunnel struct {
	client, back io.ReadWriteCloser
	mu           sync.Mutex
	downMid      bool        // the upstream is inside a fragmented message
	downPartial  bool        // a frame reached the client only in part
	refusing     atomic.Bool // an over-limit client message ends the socket
}

type wsFrameHead struct {
	fin  bool
	op   byte
	n    int64
	raw  []byte
	data [14]byte
}

func readWSFrameHead(r *bufio.Reader, h *wsFrameHead) error {
	if _, err := io.ReadFull(r, h.data[:2]); err != nil {
		return err
	}
	h.fin, h.op = h.data[0]&0x80 != 0, h.data[0]&0x0f
	m, n := 2, int64(h.data[1]&0x7f)
	switch n {
	case 126:
		m = 4
	case 127:
		m = 10
	}
	if h.data[1]&0x80 != 0 {
		m += 4 // masking key
	}
	if _, err := io.ReadFull(r, h.data[2:m]); err != nil {
		return err
	}
	switch n {
	case 126:
		n = int64(binary.BigEndian.Uint16(h.data[2:4]))
	case 127:
		u := binary.BigEndian.Uint64(h.data[2:10])
		if u > 1<<62 {
			return errors.New("websocket frame length out of range")
		}
		n = int64(u)
	}
	h.n, h.raw = n, h.data[:m]
	return nil
}

// up relays client frames to the upstream, counting each data message; the
// frame that would take a message past L is never forwarded.
func (t *wsTunnel) up(r *bufio.Reader, requestID, path string) {
	var h wsFrameHead
	var msg int64
	for readWSFrameHead(r, &h) == nil {
		if h.op < 8 { // data or continuation; control frames are at most 125 bytes
			if h.op != 0 {
				msg = 0
			}
			if msg += h.n; msg > fallbackLimit {
				log.Printf("ws native tunnel message refused: path=%s bytes>%d limit=%d request_id=%s", path, msg, fallbackLimit, requestID)
				t.refuse()
				return
			}
		}
		if _, err := t.back.Write(h.raw); err != nil {
			return
		}
		if _, err := io.CopyN(t.back, r, h.n); err != nil {
			return
		}
	}
}

// down relays upstream frames to the client, tracking message boundaries
// so keeper's error event can only be placed between messages.
func (t *wsTunnel) down(r *bufio.Reader) {
	var h wsFrameHead
	for readWSFrameHead(r, &h) == nil {
		t.mu.Lock()
		if t.refusing.Load() {
			t.mu.Unlock()
			return
		}
		t.downPartial = true
		_, err := t.client.Write(h.raw)
		if err == nil {
			_, err = io.CopyN(t.client, r, h.n)
		}
		if err == nil {
			t.downPartial = false
			if h.op < 8 {
				t.downMid = !h.fin
			}
		}
		t.mu.Unlock()
		if err != nil {
			return
		}
	}
}

// refuse ends the socket on an over-limit client message: the upstream
// connection closes without the message, and the client gets the bridge's
// named error event and a normal close, unless the upstream was mid-message
// (the event cannot be placed then; the close still names the cause).
func (t *wsTunnel) refuse() {
	t.refusing.Store(true)
	t.back.Close() // unblocks down; the upstream never sees the message
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.downPartial {
		return
	}
	if !t.downMid {
		ev, _ := json.Marshal(map[string]any{"type": "error", "status": http.StatusRequestEntityTooLarge, "error": map[string]any{
			"type": "invalid_request_error", "code": "request_too_large", "hop": keeperHop, "limit_bytes": fallbackLimit,
			"message": fmt.Sprintf("Request exceeds the %d-byte WebSocket message limit of %s.", fallbackLimit, keeperHop)}})
		if _, err := t.client.Write(wsServerFrame(0x1, ev)); err != nil {
			return
		}
	}
	_, _ = t.client.Write(wsServerFrame(0x8, append([]byte{0x03, 0xe8}, "request too large"...))) // 1000
}

// wsServerFrame encodes one unmasked, unfragmented server frame.
func wsServerFrame(op byte, payload []byte) []byte {
	b := []byte{0x80 | op}
	switch n := len(payload); {
	case n < 126:
		b = append(b, byte(n))
	case n < 1<<16:
		b = binary.BigEndian.AppendUint16(append(b, 126), uint16(n))
	default:
		b = binary.BigEndian.AppendUint64(append(b, 127), uint64(n))
	}
	return append(b, payload...)
}

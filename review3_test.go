package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

// Regression tests ported from the second independent review
// (/var/tmp/keeper-review2-20261022). Each maps to a REVIEW2 finding.

// REVIEW2-1a: cancelling the handler context must release the socket slot.
func TestReview3CancelledHandlerReleasesSockets(t *testing.T) {
	upConn := make(chan *websocket.Conn, 1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		upConn <- c
		defer c.CloseNow()
		for {
			if _, _, e = c.Read(r.Context()); e != nil {
				return
			}
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	cancelHandler := make(chan context.CancelFunc, 1)
	ended := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithCancel(r.Context())
		cancelHandler <- cancel
		k.ServeHTTP(w, r.WithContext(ctx))
		close(ended)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, _, e := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/responses", nil)
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	uc := <-upConn
	defer uc.CloseNow()
	(<-cancelHandler)()
	select {
	case <-ended:
	case <-time.After(time.Second):
		t.Fatalf("cancelled handler stuck in cleanup: occupied socket slots=%d", len(k.work.sockets))
	}
	if slots := len(k.work.sockets); slots != 0 {
		t.Fatalf("socket slot leaked after handler exit: %d", slots)
	}
}

// REVIEW2-1b: abrupt disconnects must free socket slots quickly.
func TestReview3DisconnectReleasesSocketSlots(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		for {
			if _, _, e = c.Read(r.Context()); e != nil {
				return
			}
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	for i := 0; i < 30; i++ {
		c := dialWS(t, srv, "Bearer a")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"A"}`))
		time.Sleep(2 * time.Millisecond)
		c.CloseNow()
		cancel()
	}
	deadline := time.Now().Add(2 * time.Second)
	for len(k.work.sockets) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if slots := len(k.work.sockets); slots != 0 {
		t.Fatalf("idle abrupt disconnect leaks %d socket slots", slots)
	}
}

// REVIEW2-1c: a client write failure releases the inflight spool + charge.
func TestReview3WriteErrorReleasesInflight(t *testing.T) {
	_, srv, k := newWSNativeKeeper(t)
	conn := dialWS(t, srv, "Bearer a")
	conn.CloseNow()
	raw, e := k.spoolRequest(context.Background(), strings.NewReader(`{"model":"m","input":[]}`))
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	turn, ae := k.prepareDiskEnv(context.Background(), raw, [32]byte{}, func(string) ([32]byte, bool) { return [32]byte{}, false }, true, true)
	if ae != nil {
		t.Fatal(ae)
	}
	data, e := k.spoolRequest(context.Background(), strings.NewReader(`{"type":"response.created","response":{"id":"test_id","status":"in_progress","output":[]}}`))
	if e != nil {
		t.Fatal(e)
	}
	defer data.Close()
	in := &wsNativeInflight{turn: turn}
	status := k.wsNativeUpstreamFrame(context.Background(), conn, &wsFrame{typ: websocket.MessageText, spool: data}, &wsSession{}, in)
	if status != -1 {
		t.Fatalf("want writer failure; got %d", status)
	}
	k.work.mu.Lock()
	output := k.work.output
	k.work.mu.Unlock()
	paths, _ := filepath.Glob(spoolFilePattern(k.cfg.storeDir))
	if in.c.charged != 0 || output != 0 || !turn.file.ownerClosed {
		t.Errorf("failed-write cleanup missing: collector=%d output=%d turn.closed=%v files=%d", in.c.charged, output, turn.file.ownerClosed, len(paths))
	}
	turn.file.Close()
	in.c.release(k.work)
}

// REVIEW2-2a: a CR-only event on a live stream dispatches promptly (WHATWG §9.2.6).
func TestReview3CROnlyLiveStream(t *testing.T) {
	r, w := io.Pipe()
	defer r.Close()
	defer w.Close()
	got := make(chan string, 1)
	end := make(chan error, 1)
	go func() {
		end <- readSSE(r, nil, func(b []byte) error { got <- string(b); return nil })
	}()
	go func() { _, _ = io.WriteString(w, "data: {\"type\":\"vendor.e\"}\r\r") }()
	select {
	case <-got:
	case <-time.After(500 * time.Millisecond):
		w.Close()
		<-end
		t.Fatal("CR-terminated event buffered until EOF or LF; no live dispatch")
	}
}

// REVIEW2-2b: the event bound is enforced while reading, not after an
// unbounded line allocation.
type review3CountReader struct {
	r io.Reader
	n int
}

func (r *review3CountReader) Read(p []byte) (int, error) {
	n, e := r.r.Read(p)
	r.n += n
	return n, e
}

func TestReview3SSELineBound(t *testing.T) {
	r := &review3CountReader{r: strings.NewReader("data: " + strings.Repeat("x", 8<<20) + "\n\n")}
	e := readSSE(r, nil, func([]byte) error { return nil })
	if e == nil {
		t.Fatal("oversize accepted")
	}
	if r.n > maxEventBytes+(256<<10) {
		t.Fatalf("read %d bytes before enforcing event bound %d", r.n, maxEventBytes)
	}
}

// REVIEW2-2c: mixed CR/LF/CRLF framing keeps every byte.
func TestReview3MixedSSERawPreserved(t *testing.T) {
	stream := "data: {\"type\":\"vendor.a\"}\r\revent: b\ndata: {\"type\":\"vendor.b\"}\n\n"
	var frames []string
	e := readSSE(strings.NewReader(stream), func(f []byte) error { frames = append(frames, string(f)); return nil }, func([]byte) error { return nil })
	if e != nil {
		t.Fatal(e)
	}
	if strings.Join(frames, "") != stream {
		t.Fatalf("mixed line framing changed:\nwant %q\ngot  %q", stream, strings.Join(frames, ""))
	}
}

// REVIEW2-3a: skipping a 4 MiB input to find the parent stays small.
func TestReview3ProbeMetadataHeapBound(t *testing.T) {
	_, _, k := newWSNativeKeeper(t, func(c *config) { c.maxRequestBytes = 8 << 20 })
	input := `{"type":"response.create","input":"` + strings.Repeat("x", 4<<20) + `","previous_response_id":"resp_native"}`
	data, e := k.spoolRequest(context.Background(), strings.NewReader(input))
	if e != nil {
		t.Fatal(e)
	}
	defer data.Close()
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	p := probeWSFrame(data)
	runtime.ReadMemStats(&b)
	if p.prev != "resp_native" {
		t.Fatalf("unexpected metadata: %+v", p)
	}
	if delta := b.TotalAlloc - a.TotalAlloc; delta > 1<<20 {
		t.Fatalf("metadata probe allocated %d bytes for 4MiB skipped input; not bounded-small metadata parsing", delta)
	}
}

// REVIEW2-3b: upstream event is collected once, not decoded twice.
func TestReview3SingleCollect(t *testing.T) {
	_, _, k := newWSNativeKeeper(t)
	var c collector
	defer c.release(k.work)
	calls := 0
	orig := collectEventCalls
	collectEventCalls = func() { calls++ }
	defer func() { collectEventCalls = orig }()
	raw := []byte(`{"type":"response.output_item.done","output_index":0,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"x"}]}}`)
	wire, release, err := k.responseFrame(context.Background(), &c, raw, true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(wire) == 0 {
		t.Fatal("empty wire")
	}
	if calls != 1 {
		t.Fatalf("event collected %d times", calls)
	}
}

// REVIEW2-4: a non-create frame while a turn runs must not evict the queued create.
func TestReview3NonCreatePreservesQueuedCreate(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		_, _, e = c.Read(r.Context())
		if e != nil {
			return
		}
		_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.created","response":{"id":"q1","status":"in_progress","output":[]}}`))
		// While turn A is active, only the opaque non-create frame should reach upstream.
		_, _, e = c.Read(r.Context())
		if e != nil {
			return
		}
		_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.completed","response":{"id":"q1","status":"completed","output":[]}}`))
		for {
			if _, _, e = c.Read(r.Context()); e != nil {
				return
			}
			_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"response.completed","response":{"id":"q2","status":"completed","output":[]}}`))
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","input":"A"}`))
	if _, _, e := c.Read(ctx); e != nil {
		t.Fatal(e)
	}
	_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","input":"B"}`))
	time.Sleep(10 * time.Millisecond)
	_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"vendor.telemetry"}`))
	for i := 0; i < 3; i++ {
		_, b, e := c.Read(ctx)
		if e != nil {
			t.Fatalf("queued B was not completed: %v", e)
		}
		if strings.Contains(string(b), `"type":"error"`) {
			t.Fatalf("non-create frame incorrectly evicted queued create: %s", b)
		}
		if strings.Contains(string(b), "q2") {
			return
		}
	}
	t.Fatal("queued B never completed")
}

// REVIEW2-5: unknown events with a nested response stay byte-for-byte.
func TestReview3UnknownNestedEventUnchanged(t *testing.T) {
	_, _, k := newWSNativeKeeper(t)
	raw := []byte(`{ "type":"vendor.telemetry", "response":{"id":"trace_7","status":"active"}, "sequence_number":42}`)
	var c collector
	defer c.release(k.work)
	wire, release, e := k.responseFrame(context.Background(), &c, raw, true, true, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if !bytes.Equal(raw, wire) {
		t.Fatalf("unknown event rewritten: %s", wire)
	}
}

// REVIEW2-6: invalid UTF-8 from upstream is not forwarded; upstream conn dies 1007.
func TestReview3UpstreamUTF8(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		if _, _, e = c.Read(r.Context()); e != nil {
			return
		}
		_ = c.Write(r.Context(), websocket.MessageText, []byte{0xff})
		for {
			if _, _, e = c.Read(r.Context()); e != nil {
				return
			}
		}
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","previous_response_id":"resp_native","input":[]}`))
	_, b, e := c.Read(ctx)
	if e == nil && !utf8.Valid(b) {
		t.Fatalf("keeper forwarded invalid UTF8 upstream text to client: %x", b)
	}
	if websocket.CloseStatus(e) != websocket.StatusInvalidFramePayloadData {
		t.Fatalf("got %v (%x), expected 1007", e, b)
	}
}

// REVIEW2-7: a refused-upgrade body above the bound fails explicitly.
func TestReview3RefusedBodyExceedingBoundFailsExplicitly(t *testing.T) {
	body := strings.Repeat("x", maxResponseBytes+1)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		io.WriteString(w, body)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	req := httptest.NewRequest("GET", "/v1/responses", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	rec := httptest.NewRecorder()
	k.ServeHTTP(rec, req)
	if rec.Code == 403 && rec.Body.String() != body {
		t.Fatalf("refused body silently truncated: status=%d len=%d of %d; expected complete body or explicit capacity error", rec.Code, rec.Body.Len(), len(body))
	}
}

// REVIEW2-8: a response the keeper could not retain keeps its upstream id.
func TestReview3StoreFailureDoesNotRewriteUnrecordedID(t *testing.T) {
	_, srv, k := newWSNativeKeeper(t, func(c *config) { c.maxStoreBytes = 1; c.storePinBytes = 1 })
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`)
	id := responseID(ev)
	if n, _ := k.store.stats(); n != 0 {
		t.Fatalf("fixture stored %d", n)
	}
	if id != "wsid_1" {
		t.Fatalf("unrecorded response id rewritten: %s", id)
	}
}

// Raw-wire helpers shared by the RFC 6455 regression tests.
func review3RawWrite(c net.Conn, fin bool, op byte, p []byte) error {
	h := []byte{op, 0x80 | byte(len(p))}
	if fin {
		h[0] |= 0x80
	}
	mask := []byte{1, 2, 3, 4}
	h = append(h, mask...)
	for i, b := range p {
		h = append(h, b^mask[i%4])
	}
	_, e := c.Write(h)
	return e
}

func review3RawRead(r *bufio.Reader) (byte, []byte, error) {
	h := make([]byte, 2)
	if _, e := io.ReadFull(r, h); e != nil {
		return 0, nil, e
	}
	n := int(h[1] & 127)
	if n == 126 {
		b := make([]byte, 2)
		if _, e := io.ReadFull(r, b); e != nil {
			return 0, nil, e
		}
		n = int(binary.BigEndian.Uint16(b))
	}
	p := make([]byte, n)
	_, e := io.ReadFull(r, p)
	return h[0] & 15, p, e
}

// REVIEW2-9: real wire fragmentation, an interleaved ping, and a 1000 close.
func TestReview3RealFragmentPingAndClose(t *testing.T) {
	_, srv, _ := newWSNativeKeeper(t)
	u, _ := url.Parse(srv.URL)
	c, e := net.Dial("tcp", u.Host)
	if e != nil {
		t.Fatal(e)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(3 * time.Second))
	fmt.Fprintf(c, "GET /v1/responses HTTP/1.1\r\nHost: %s\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", u.Host)
	br := bufio.NewReader(c)
	resp, e := http.ReadResponse(br, nil)
	if e != nil || resp.StatusCode != 101 {
		t.Fatalf("handshake %+v %v", resp, e)
	}
	if e = review3RawWrite(c, false, 1, []byte(`{"type":"response.create",`)); e != nil {
		t.Fatal(e)
	}
	_ = review3RawWrite(c, true, 9, []byte("probe"))
	op, p, e := review3RawRead(br)
	if e != nil || op != 10 || string(p) != "probe" {
		t.Fatalf("interleaved pong op=%d p=%q err=%v", op, p, e)
	}
	_ = review3RawWrite(c, true, 0, []byte(`"model":"m","input":[]}`))
	for {
		op, p, e = review3RawRead(br)
		if e != nil {
			t.Fatal(e)
		}
		if op == 1 && strings.Contains(string(p), "response.completed") {
			break
		}
	}
	_ = review3RawWrite(c, true, 8, []byte{3, 232})
	op, p, e = review3RawRead(br)
	if e != nil || op != 8 || len(p) < 2 || binary.BigEndian.Uint16(p) != 1000 {
		t.Fatalf("close op=%d p=%q err=%v", op, p, e)
	}
}

// REVIEW2-10: non-Responses SSE stays byte-for-byte through the native proxy.
func TestReview3NonResponsesSSEUnchanged(t *testing.T) {
	for _, path := range []string{"/v1/messages", "/v1/chat/completions"} {
		t.Run(path, func(t *testing.T) {
			stream := "event: ping\r\ndata: {\"type\":\"ping\"}\r\n\r\nevent: message_stop\ndata: {}\n\ndata: [DONE]\n\n"
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				io.WriteString(w, stream)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
			resp, e := http.Post(srv.URL+path, "application/json", strings.NewReader(`{"model":"m"}`))
			if e != nil {
				t.Fatal(e)
			}
			defer resp.Body.Close()
			b, _ := io.ReadAll(resp.Body)
			if string(b) != stream {
				t.Fatalf("non-Responses stream changed: %q", b)
			}
			if n, _ := k.store.stats(); n != 0 {
				t.Fatalf("non-Responses traffic stored: %d", n)
			}
		})
	}
}

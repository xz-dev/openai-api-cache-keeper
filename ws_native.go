package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/coder/websocket"
)

// wsFrame is one spooled WebSocket message.
type wsFrame struct {
	typ   websocket.MessageType
	spool *spool
}

// wsRefusedHandshake holds a non-101 upgrade response's spooled body.
type wsRefusedHandshake struct {
	body     io.ReadCloser
	overflow bool // body exceeded maxResponseBytes; refusing silent truncation
	failed   bool // body capture failed; refusing silent truncation
}

// wsHandshakeBody is a RoundTripper wrapper for the upstream WS handshake:
// for a non-101 upgrade response it spools the entire body (bounded by
// maxResponseBytes) so the client sees the upstream's rejection verbatim.
// coder/websocket truncates the body it returns to 1024 bytes for debugging;
// the spool here is what serveWSNative actually writes back.
type wsHandshakeBody struct {
	base http.RoundTripper
	keep **wsRefusedHandshake
	k    *keeper
}

func (t *wsHandshakeBody) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp.StatusCode == http.StatusSwitchingProtocols || resp.Body == nil {
		return resp, err
	}
	// Only upgrade handshakes interest us; everything else is untouched.
	if !isUpgradeRequest(req) {
		return resp, nil
	}
	body := &wsRefusedHandshake{}
	spool, err := t.k.newSpool()
	if err != nil {
		// No spool: refuse the silent 1024-byte truncation explicitly.
		body.failed = true
		*t.keep = body
		return resp, nil
	}
	// Read into the spool while still returning the original stream to
	// websocket.Dial, which consumes its own 1024-byte diagnostic prefix.
	// A tee read or drain error marks capture failed: replaying the partial
	// spool would silently present a truncated rejection as the real one.
	orig := resp.Body
	tee := io.TeeReader(io.LimitReader(orig, maxResponseBytes+1), struct{ io.Writer }{spool})
	resp.Body = struct {
		io.Reader
		io.Closer
	}{Reader: struct{ io.Reader }{tee}, Closer: closerFunc(func() error {
		// Drain what Dial's diagnostic read did not, so the whole body is
		// captured before serveWSNative replays it. TeeReader stops writing
		// to the spool after its first write error, so io.Copy returning nil
		// still leaves spool.size short of the real body — compare against
		// the source, which is the only complete-length signal we have.
		_, copyErr := io.Copy(struct{ io.Writer }{spool}, io.LimitReader(orig, maxResponseBytes+1-spool.size))
		closeErr := orig.Close()
		// The spool's byte accounting returns a short count on its own write
		// failure (TeeReader then keeps reading without writing), so a nil
		// copyErr can still mean the capture is short. When the upstream
		// declared a length, anything less is a failed capture, never a
		// complete rejection to replay.
		incomplete := resp.ContentLength >= 0 && spool.size != resp.ContentLength
		if copyErr != nil || closeErr != nil || incomplete || spool.size > maxResponseBytes {
			spool.Close()
			if spool.size > maxResponseBytes {
				body.overflow = true
			} else {
				body.failed = true
			}
			*t.keep = body
			return nil
		}
		if rb, err := spool.requestBody(); err == nil {
			body.body = rb
			// The reader holds its own ref; the owner ref is released so the
			// file is removed once the replay finishes.
			spool.Close()
		} else {
			spool.Close()
			body.failed = true
		}
		*t.keep = body
		return nil
	})}
	return resp, nil
}

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

// serveWSNative terminates the client WebSocket and relays it over a second
// WebSocket to the single configured upstream. Only response.create text
// frames are inspected, once per turn:
//
//   - previous_response_id owned by the keeper for this caller: the frame is
//     expanded to the full stored input with the parent field removed, and the
//     turn is recorded like a local turn (ids rewritten, same-socket pin).
//   - No parent: the frame is forwarded semantically unchanged and the turn is
//     recorded, so the produced id becomes continuable.
//   - Any other previous_response_id: the frame and the turn pass through
//     byte-for-byte; the downstream owns that cache. Keeper-owned ids that are
//     expired or foreign fail closed instead of leaking a stripped delta.
//
// Turns serialize exactly once: while a turn is in flight, later create frames
// wait. Every other frame — upstream or client — relays immediately.
func (k *keeper) serveWSNative(w http.ResponseWriter, r *http.Request) {
	if !trySlot(k.work.sockets) {
		writeAPIError(w, workingError(errors.New("socket limit")))
		return
	}
	defer func() { <-k.work.sockets }()

	target := *k.cfg.upstream
	target.Path = strings.TrimRight(target.Path, "/") + r.URL.Path
	target.RawQuery = r.URL.RawQuery
	switch strings.ToLower(target.Scheme) {
	case "http", "ws":
		target.Scheme = "ws"
	case "https", "wss":
		target.Scheme = "wss"
	default:
		target.Scheme = "ws"
	}
	header := http.Header{}
	copyHeader(header, r.Header)
	for name := range header {
		if strings.HasPrefix(strings.ToLower(name), "sec-websocket-") {
			header.Del(name)
		}
	}
	var subprotocols []string
	for _, h := range r.Header.Values("Sec-WebSocket-Protocol") {
		for _, p := range strings.Split(h, ",") {
			if p = strings.TrimSpace(p); p != "" {
				subprotocols = append(subprotocols, p)
			}
		}
	}
	dialCtx, dialCancel := context.WithTimeout(r.Context(), 30*time.Second)
	// coder/websocket keeps only a 1024-byte diagnostic prefix of a refused
	// upgrade's body. Intercept the handshake response in the transport so
	// the client's rejection reply keeps the upstream status, headers and
	// full body (bounded by maxResponseBytes like error bodies elsewhere).
	var refused *wsRefusedHandshake
	dialClient := *k.client
	dialClient.Transport = &wsHandshakeBody{base: k.client.Transport, keep: &refused, k: k}
	upstream, resp, err := websocket.Dial(dialCtx, target.String(), &websocket.DialOptions{
		HTTPClient:   &dialClient,
		HTTPHeader:   header,
		Subprotocols: subprotocols,
	})
	dialCancel()
	if err != nil {
		// A refused upgrade keeps the upstream status, headers and body.
		// A body that overflowed or could not be captured is an explicit
		// upstream error instead of a silently truncated reply.
		if refused != nil && (refused.overflow || refused.failed) {
			writeAPIError(w, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream rejection body exceeded working limit or could not be read"})
			return
		}
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			copyHeader(w.Header(), resp.Header)
			w.Header().Del("Content-Length")
			w.WriteHeader(resp.StatusCode)
			if refused != nil && refused.body != nil {
				defer refused.body.Close()
				_, _ = io.Copy(w, refused.body)
			} else if resp.Body != nil {
				_, _ = io.Copy(w, resp.Body)
			}
			return
		}
		writeAPIError(w, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"})
		return
	}
	defer upstream.CloseNow()
	upstream.SetReadLimit(k.cfg.maxRequestBytes)

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: true,
		Subprotocols:       []string{upstream.Subprotocol()},
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(k.cfg.maxRequestBytes)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	owner := k.ownerOf(r)
	sess := &wsSession{}
	defer sess.clear()

	// One pump result per direction carries a read error (including a close
	// frame's status) so the handler can perform the counterpart close
	// handshake instead of dropping the connection silently.
	type pumpResult struct {
		frame wsFrame
		err   error // nil while frames flow
	}
	upCh := make(chan pumpResult, 8)
	clCh := make(chan pumpResult, 1)

	// Upstream → client pump. Readers get a non-cancellable context: the
	// library closes the whole connection when a read context ends
	// (setupReadTimeout), which would silently kill the socket before the
	// peer's close status can be relayed. Socket teardown always goes
	// through CloseNow/Close, which unblocks these readers.
	go func() {
		defer close(upCh)
		defer cancel()
		for {
			typ, reader, err := upstream.Reader(context.Background())
			if err != nil {
				select {
				case upCh <- pumpResult{err: err}:
				case <-ctx.Done():
				}
				return
			}
			f, err := k.spoolRequestUTF8(ctx, reader, typ)
			if err != nil {
				if errors.Is(err, errInvalidUTF8) {
					// RFC 6455 §8.1: invalid UTF-8 in a text message fails the
					// connection with 1007 in the keeper's server role too.
					_ = upstream.Close(websocket.StatusInvalidFramePayloadData, "invalid UTF-8")
					upCh <- pumpResult{err: err}
					return
				}
				if readDiscard(ctx, reader) != nil {
					upCh <- pumpResult{err: errors.New("upstream read failed")}
					return
				}
				// Oversized upstream frames cannot be collected or safely
				// relayed; end the turn in protocol terms and close.
				log.Printf("ws native upstream frame dropped: %v", err)
				upCh <- pumpResult{err: err}
				return
			}
			select {
			case upCh <- pumpResult{frame: wsFrame{typ, f}}:
			case <-ctx.Done():
				f.Close()
				return
			}
		}
	}()

	_ = ctx // reads are Background-bound; ctx governs handler lifetime only

	// Client → upstream pump: one queued message per socket plus the in-flight
	// turn, matching the local-mode socket bound.
	go func() {
		defer close(clCh)
		defer cancel()
		for {
			typ, reader, err := conn.Reader(context.Background())
			if err != nil {
				select {
				case clCh <- pumpResult{err: err}:
				case <-ctx.Done():
				}
				return
			}
			if !trySlot(k.work.turns) {
				if readDiscard(ctx, reader) != nil {
					clCh <- pumpResult{err: errors.New("client read failed")}
					return
				}
				wsError(ctx, conn, workingError(errors.New("turn limit")))
				continue
			}
			f, err := k.spoolRequestUTF8(ctx, reader, typ)
			if err != nil {
				<-k.work.turns
				if errors.Is(err, errInvalidUTF8) {
					// RFC 6455 §8.1: invalid UTF-8 in a text message fails
					// the connection with 1007.
					_ = conn.Close(websocket.StatusInvalidFramePayloadData, "invalid UTF-8")
					clCh <- pumpResult{err: err}
					return
				}
				wsError(ctx, conn, workingError(err))
				if readDiscard(ctx, reader) != nil {
					clCh <- pumpResult{err: err}
					return
				}
				continue
			}
			select {
			case clCh <- pumpResult{frame: wsFrame{typ, f}}:
			case <-ctx.Done():
				f.Close()
				<-k.work.turns
				return
			default:
				f.Close()
				<-k.work.turns
				wsError(ctx, conn, workingError(errors.New("socket queue full")))
			}
		}
	}()

	var inflight *wsNativeInflight // recorded turn awaiting its terminal frame
	var nativeOpen bool            // native create forwarded, terminal not seen
	var queued *wsFrame            // one create frame held while a turn runs

	// releaseInflight frees the recorded turn's spool and collector charge on
	// every early exit; callers clear the pointer only after it returns.
	releaseInflight := func() {
		if inflight != nil {
			inflight.turn.file.Close()
			inflight.c.release(k.work)
			inflight = nil
		}
	}

	// cleanup releases every resource owned by this handler on every exit
	// path: the in-flight turn spool and collector charge, a held queued
	// frame, and any frames still sitting in a pump channel. It must only run
	// after BOTH sockets are closed: each pump reads with
	// context.Background(), so its Reader returns only when its own socket
	// dies. Ranging a pump channel before that deadlocks and leaks the
	// socket slot. It drains the original channels captured at pump start,
	// not the select variables the handler nils on shutdown.
	pumpUp, pumpCl := upCh, clCh
	cleanup := func() {
		releaseInflight()
		if queued != nil {
			queued.spool.Close()
			<-k.work.turns
			queued = nil
		}
		for res := range pumpUp {
			if res.frame.spool != nil {
				res.frame.spool.Close()
			}
		}
		for res := range pumpCl {
			if res.frame.spool != nil {
				res.frame.spool.Close()
				<-k.work.turns
			}
		}
	}

	processClient := func(f *wsFrame) {
		start := time.Now()
		id, status, hasPrev := k.wsNativeClientFrame(ctx, conn, upstream, r, owner, f, sess, &inflight, &nativeOpen)
		f.spool.Close()
		<-k.work.turns
		logTurn("ws", status, hasPrev, id, start, k.store)
	}
	processUpstream := func(f *wsFrame) {
		defer f.spool.Close()
		if f.typ != websocket.MessageText {
			if err := k.wsRelayFrame(ctx, conn, f.spool, f.typ); err != nil {
				cancel()
			}
			return
		}
		if inflight != nil {
			if status := k.wsNativeUpstreamFrame(ctx, conn, f, sess, inflight); status != 0 {
				inflight = nil
				if status == -1 {
					cancel()
					return
				}
			}
			return
		}
		terminal := false
		if nativeOpen {
			terminal = wsNativeTerminal(f.spool)
		}
		if err := k.wsRelayFrame(ctx, conn, f.spool, f.typ); err != nil {
			cancel()
			return
		}
		if terminal {
			nativeOpen = false
		}
	}

	// pendingFrame releases the frame carried by a pump result that arrived
	// too late to be processed (cancellation race).
	pendingFrame := func(res pumpResult) {
		if res.frame.spool != nil {
			res.frame.spool.Close()
		}
	}
	for {
		if queued != nil && inflight == nil && !nativeOpen {
			f := queued
			queued = nil
			processClient(f)
		}
		select {
		case res, ok := <-upCh:
			if !ok {
				// Channel closed after pump return without an error result;
				// treat like a transport EOF.
				upCh = nil
				conn.CloseNow()
				upstream.CloseNow()
				cleanup()
				return
			}
			if res.err != nil {
				// Propagate the downstream close status to the client when
				// it was a clean close handshake (RFC 6455 §7.1).
				var ce websocket.CloseError
				switch {
				case errors.As(res.err, &ce):
					_ = conn.Close(ce.Code, ce.Reason)
				case errors.Is(res.err, errInvalidUTF8):
					_ = conn.Close(websocket.StatusInvalidFramePayloadData, "invalid UTF-8")
				}
				upCh = nil
				conn.CloseNow()
				upstream.CloseNow()
				cleanup()
				return
			}
			f := res.frame
			processUpstream(&f)
		case res, ok := <-clCh:
			if !ok {
				clCh = nil
				conn.CloseNow()
				upstream.CloseNow()
				cleanup()
				return
			}
			if res.err != nil {
				var ce websocket.CloseError
				if errors.As(res.err, &ce) {
					_ = upstream.Close(ce.Code, ce.Reason)
				}
				clCh = nil
				conn.CloseNow()
				upstream.CloseNow()
				cleanup()
				return
			}
			f := res.frame
			if queued == nil && inflight == nil && !nativeOpen {
				processClient(&f)
				continue
			}
			// Classify before touching the create queue: a non-create frame
			// has no turn ordering and relays immediately; it must never
			// evict the held create.
			if !isWSCreate(f.spool) {
				if err := k.wsRelayFrame(ctx, upstream, f.spool, f.typ); err != nil {
					f.spool.Close()
					<-k.work.turns
					clCh = nil
					conn.CloseNow()
					upstream.CloseNow()
					cleanup()
					cancel()
					return
				}
				f.spool.Close()
				<-k.work.turns
				continue
			}
			// Hold at most one create while a turn is in flight; a second
			// queued create is shed with an explicit overload error.
			if queued != nil {
				f.spool.Close()
				<-k.work.turns
				wsError(ctx, conn, workingError(errors.New("socket queue full")))
				continue
			}
			held := f
			queued = &held
		case <-ctx.Done():
			// A pump may have queued its terminal error result just before
			// cancelling; prefer propagating its close status over a bare
			// drop so the peer still sees the real status/reason. Any frame
			// carried by that result is released, never processed.
			select {
			case res := <-upCh:
				var ce websocket.CloseError
				if errors.As(res.err, &ce) {
					_ = conn.Close(ce.Code, ce.Reason)
				}
				pendingFrame(res)
			default:
			}
			select {
			case res := <-clCh:
				var ce websocket.CloseError
				if errors.As(res.err, &ce) {
					_ = upstream.Close(ce.Code, ce.Reason)
				}
				if res.frame.spool != nil {
					res.frame.spool.Close()
					<-k.work.turns
				}
			default:
			}
			// Kill both sockets first: the pumps read with
			// context.Background(), so their Readers return only after this,
			// closing the channels cleanup drains.
			conn.CloseNow()
			upstream.CloseNow()
			cleanup()
			return
		}
	}
}

// wsNativeInflight tracks one keeper-recorded WS turn: the expanded request on
// disk plus the collector for the upstream event frames.
type wsNativeInflight struct {
	c      collector
	turn   *diskTurn
	owner  [32]byte
	sess   *wsSession
	id     string // stored public id, set once accepted
	failed bool
}

// wsProbe is the bounded top-level metadata a client frame needs before it is
// classified: the turn type, any parent id, and the generate flag. Only the
// scalar values are retained; everything else is skipped token by token so a
// multi-megabyte input never decodes to heap.
type wsProbe struct {
	typeSet, isCreate bool
	prev              string
	prevSet           bool
	generate          bool
	generateSet       bool
}

// probeWSFrame scans the spooled frame for wsProbe fields with the span
// scanner: a multi-megabyte input is skipped as a file span, never decoded
// to heap.
func probeWSFrame(data *spool) wsProbe {
	var p wsProbe
	s := spoolScanner(data)
	err := s.objectFields(data.keySlice, func(key string, start, end int64) error {
		switch key {
		case "type":
			if raw := data.keySlice(start, end); raw != nil {
				if v, ok := readJSONString(raw); ok {
					p.typeSet = true
					p.isCreate = v == "response.create"
				}
			}
		case "previous_response_id":
			if raw := data.keySlice(start, end); raw != nil {
				if v, ok := readJSONString(raw); ok && v != "" {
					p.prev, p.prevSet = v, true
				}
			}
		case "generate":
			if raw := data.keySlice(start, end); raw != nil {
				var b bool
				if json.Unmarshal(raw, &b) == nil {
					p.generate, p.generateSet = b, true
				}
			}
		}
		return nil
	})
	if err != nil {
		return wsProbe{}
	}
	return p
}

// isWSCreate reports whether the spooled frame is a response.create object.
func isWSCreate(data *spool) bool {
	return probeWSFrame(data).isCreate
}

// wsNativeTerminal reports whether the spooled upstream text frame ends a
// native turn (terminal response event or a request-level error frame).
// It scans only the type field; the rest is skipped as spans.
func wsNativeTerminal(data *spool) bool {
	var kind string
	s := spoolScanner(data)
	err := s.objectFields(data.keySlice, func(key string, start, end int64) error {
		if key == "type" {
			if raw := data.keySlice(start, end); raw != nil {
				kind, _ = readJSONString(raw)
			}
		}
		return nil
	})
	if err != nil {
		return true // unparseable: stop attributing frames to this turn
	}
	switch kind {
	case "response.completed", "response.incomplete", "response.failed", "error":
		return true
	}
	return false
}

// utf8CleanReader wraps an io.Reader and validates that all bytes read form
// valid UTF-8 across chunk boundaries (RFC 6455 §5.6/§8.1). It returns
// errInvalidUTF8 once the first malformed sequence is seen.
type utf8CleanReader struct {
	r    io.Reader
	tail []byte // at most 3 bytes, a prefix of a possibly-split rune
	done bool
}

var errInvalidUTF8 = errors.New("invalid UTF-8 in text message")

func (u *utf8CleanReader) Read(p []byte) (int, error) {
	if u.done {
		return 0, io.EOF
	}
	n, err := u.r.Read(p)
	// Validate tail+chunk rune by rune; an incomplete trailing sequence is
	// held over to the next Read, a malformed one fails the message.
	data := p[:n]
	if len(u.tail) > 0 {
		u.tail = append(u.tail, data...)
		data = u.tail
		u.tail = nil
	}
	consumed := 0
	for consumed < len(data) {
		rest := data[consumed:]
		if !utf8.FullRune(rest) {
			break // incomplete trailing sequence; needs more bytes
		}
		r, size := utf8.DecodeRune(rest)
		if r == utf8.RuneError && size <= 1 {
			return n, errInvalidUTF8
		}
		consumed += size
	}
	if consumed < len(data) {
		u.tail = append(u.tail[:0], data[consumed:]...)
	}
	if err == io.EOF {
		u.done = true
		if len(u.tail) > 0 {
			return n, errInvalidUTF8
		}
	}
	return n, err
}

// spoolRequestUTF8 spools a text message while validating UTF-8; binary and
// oversized messages behave like spoolRequest.
func (k *keeper) spoolRequestUTF8(ctx context.Context, r io.Reader, typ websocket.MessageType) (*spool, error) {
	if typ == websocket.MessageText {
		r = &utf8CleanReader{r: r}
	}
	return k.spoolRequest(ctx, r)
}

// wsNativeClientFrame classifies and forwards one client frame.
func (k *keeper) wsNativeClientFrame(ctx context.Context, conn, upstream *websocket.Conn, r *http.Request, owner [32]byte, f *wsFrame, sess *wsSession, inflight **wsNativeInflight, nativeOpen *bool) (string, int, bool) {
	data := f.spool
	if f.typ != websocket.MessageText {
		// Binary/opaque frames relay verbatim.
		if err := k.wsRelayFrame(ctx, upstream, data, f.typ); err != nil {
			return "", http.StatusBadGateway, false
		}
		return "", http.StatusOK, false
	}
	probe := probeWSFrame(data)
	if !probe.isCreate {
		if err := k.wsRelayFrame(ctx, upstream, data, f.typ); err != nil {
			return "", http.StatusBadGateway, false
		}
		return "", http.StatusOK, false
	}
	if probe.prevSet {
		known, valid := k.store.ownedID(probe.prev, owner)
		if !valid {
			// A still-live socket pin resolves before global admission so a
			// store:false turn stays continuable on its own socket.
			pinned := probe.prev == sess.lastID && sess.lastPin != ""
			if pinned {
				if _, _, ok := k.store.resolveID(sess.lastPin, owner, true); ok {
					valid = true
				}
			}
			if !valid {
				if known || pinned || strings.HasPrefix(probe.prev, keeperResponsePrefix) || probe.prev == sess.lastID {
					return "", wsError(ctx, conn, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", code: "previous_response_not_found", param: "previous_response_id",
						message: fmt.Sprintf("Previous response with id '%s' not found.", probe.prev)}), true
				}
				// Native parent: the delta is meaningful only downstream.
				// Forward verbatim and mark the turn so its events stay
				// untouched.
				if err := k.wsRelayFrame(ctx, upstream, data, f.typ); err != nil {
					return "", http.StatusBadGateway, true
				}
				*nativeOpen = true
				return "", http.StatusOK, true
			}
		}
	}
	// No parent, or a keeper-owned/pinned parent: build the upstream frame on
	// disk (expanding stored history for an owned parent) and record the turn.
	lookup := func(id string) ([32]byte, bool) {
		if id == sess.lastID && sess.lastPin != "" {
			if tip, _, ok := k.store.resolveID(sess.lastPin, owner, true); ok {
				return tip, true
			}
		}
		tip, _, ok := k.store.resolveID(id, owner, false)
		return tip, ok
	}
	leave, err := k.work.enter(ctx)
	if err != nil {
		return "", wsError(ctx, conn, workingError(err)), probe.prevSet
	}
	t, apiErr := k.prepareDiskEnv(ctx, data, owner, lookup, true, true)
	leave()
	if apiErr != nil {
		return "", wsError(ctx, conn, apiErr), probe.prevSet
	}
	body, err := t.file.requestBody()
	if err != nil {
		t.file.Close()
		return "", wsError(ctx, conn, workingError(err)), probe.prevSet
	}
	w, err := upstream.Writer(ctx, websocket.MessageText)
	if err != nil {
		body.Close()
		t.file.Close()
		return "", wsError(ctx, conn, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"}), probe.prevSet
	}
	_, copyErr := io.Copy(struct{ io.Writer }{w}, body)
	closeErr := w.Close()
	body.Close()
	if copyErr != nil || closeErr != nil {
		t.file.Close()
		return "", wsError(ctx, conn, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"}), probe.prevSet
	}
	*inflight = &wsNativeInflight{turn: t, owner: owner, sess: sess}
	return "", http.StatusOK, probe.prevSet
}

// wsNativeUpstreamFrame relays one upstream text frame through the collector,
// rewriting ids and store as local turns do. On the terminal frame it stores
// input+output, sets the session pin, and returns the stored public id; a
// non-zero status marks the turn finished (-1: connection-level failure).
// Every non-zero return releases the turn's spool and collector charge, so
// the caller only clears its pointer.
func (k *keeper) wsNativeUpstreamFrame(ctx context.Context, conn *websocket.Conn, f *wsFrame, sess *wsSession, inflight *wsNativeInflight) (status int) {
	data := f.spool
	endTurn := func(code int) int {
		inflight.failed = code == -1
		inflight.turn.file.Close()
		inflight.c.release(k.work)
		return code
	}
	// The event is the smallest unit: copy it once into a charged buffer so
	// rewrite and collect share one bounded copy instead of two.
	raw, err := data.chargedCopy(k.work)
	if err != nil {
		log.Printf("ws native upstream frame uncharged: %v", err)
		return endTurn(-1)
	}
	defer k.work.freeOutput(int64(cap(raw)))
	terminal := wsNativeTerminal(data)
	accept := func() (bool, error) {
		if rid, output, ok := inflight.c.final(); ok && inflight.id == "" {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			pin, err := k.acceptDisk(ctx, inflight.turn, publicID(rid), inflight.owner, output, sess.lastPin, true)
			if err != nil {
				logNotRetained(err)
				return false, nil
			}
			inflight.id = publicID(rid)
			sess.lastID, sess.lastPin = inflight.id, pin
			sess.release = func() {
				if err := k.store.release(pin); err != nil {
					log.Printf("socket pin cleanup failed (records remain charged): %v", err)
				}
			}
		}
		return true, nil
	}
	wire, release, err := k.responseFrame(ctx, &inflight.c, raw, true, inflight.turn.store, accept)
	if err != nil {
		log.Printf("ws native upstream frame failed: %v", err)
		return endTurn(-1)
	}
	defer release()
	if err := conn.Write(ctx, websocket.MessageText, wire); err != nil {
		return endTurn(-1)
	}
	if terminal {
		return endTurn(http.StatusOK)
	}
	return 0
}

// chargedCopy reads the whole spool once into an output-charged buffer; the
// caller frees it. Bounds the one unavoidable per-event materialization.
func (s *spool) chargedCopy(m *workingSet) ([]byte, error) {
	if s.size > maxEventBytes {
		return nil, errWorkSize
	}
	if !m.reserveOutput(s.size) {
		return nil, errWorkSize
	}
	raw := make([]byte, s.size)
	if _, err := s.ReadAt(raw, 0); err != nil {
		m.freeOutput(s.size)
		return nil, err
	}
	return raw, nil
}

// wsRelayFrame forwards one spooled message verbatim over an upstream or
// client socket.
func (k *keeper) wsRelayFrame(ctx context.Context, dst *websocket.Conn, data *spool, typ websocket.MessageType) error {
	body, err := data.requestBody()
	if err != nil {
		return err
	}
	defer body.Close()
	w, err := dst.Writer(ctx, typ)
	if err != nil {
		return err
	}
	if _, err := io.Copy(struct{ io.Writer }{w}, body); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}

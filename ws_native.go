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

	"github.com/coder/websocket"
)

// wsFrame is one spooled WebSocket message.
type wsFrame struct {
	typ   websocket.MessageType
	spool *spool
}

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
	upstream, resp, err := websocket.Dial(dialCtx, target.String(), &websocket.DialOptions{
		HTTPClient:   k.client,
		HTTPHeader:   header,
		Subprotocols: subprotocols,
	})
	dialCancel()
	if err != nil {
		// A refused upgrade keeps the upstream status, headers and body.
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			copyHeader(w.Header(), resp.Header)
			w.WriteHeader(resp.StatusCode)
			if resp.Body != nil {
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

	// Upstream → client pump. Text frames are delivered to the turn handler so
	// a recorded turn can collect and rewrite them; everything else relays.
	upCh := make(chan wsFrame, 8)
	go func() {
		defer close(upCh)
		defer cancel()
		for {
			typ, reader, err := upstream.Reader(ctx)
			if err != nil {
				return
			}
			f, err := k.spoolRequest(ctx, reader)
			if err != nil {
				if readDiscard(ctx, reader) != nil {
					return
				}
				// Oversized upstream frames cannot be collected or safely relayed.
				log.Printf("ws native upstream frame dropped: %v", err)
				continue
			}
			select {
			case upCh <- wsFrame{typ, f}:
			case <-ctx.Done():
				f.Close()
				return
			}
		}
	}()

	// Client → upstream pump: one queued message per socket plus the in-flight
	// turn, matching the local-mode socket bound.
	clCh := make(chan wsFrame, 1)
	go func() {
		defer close(clCh)
		defer cancel()
		for {
			typ, reader, err := conn.Reader(ctx)
			if err != nil {
				return
			}
			if !trySlot(k.work.turns) {
				if readDiscard(ctx, reader) != nil {
					return
				}
				wsError(ctx, conn, workingError(errors.New("turn limit")))
				continue
			}
			f, err := k.spoolRequest(ctx, reader)
			if err != nil {
				<-k.work.turns
				wsError(ctx, conn, workingError(err))
				if readDiscard(ctx, reader) != nil {
					return
				}
				continue
			}
			select {
			case clCh <- wsFrame{typ, f}:
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
	for {
		if queued != nil && inflight == nil && !nativeOpen {
			f := queued
			queued = nil
			processClient(f)
		}
		select {
		case f, ok := <-upCh:
			if !ok {
				return
			}
			processUpstream(&f)
		case f, ok := <-clCh:
			if !ok {
				return
			}
			if queued == nil && inflight == nil && !nativeOpen {
				processClient(&f)
				continue
			}
			// Hold at most one create while a turn is in flight; the reader
			// already bounds the channel, so a second queued frame is shed.
			if queued != nil {
				queued.spool.Close()
				<-k.work.turns
				wsError(ctx, conn, workingError(errors.New("socket queue full")))
			}
			if !isWSCreate(f.spool) {
				// Non-create frames have no turn ordering: relay immediately.
				if err := k.wsRelayFrame(ctx, upstream, f.spool, f.typ); err != nil {
					f.spool.Close()
					<-k.work.turns
					cancel()
					continue
				}
				f.spool.Close()
				<-k.work.turns
				continue
			}
			held := f
			queued = &held
		case <-ctx.Done():
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

// isWSCreate reports whether the spooled frame is a response.create object.
func isWSCreate(data *spool) bool {
	var probe struct {
		Type string `json:"type"`
	}
	err := json.NewDecoder(io.NewSectionReader(data.File, data.offset, data.size)).Decode(&probe)
	return err == nil && probe.Type == "response.create"
}

// wsNativeTerminal reports whether the spooled upstream text frame ends a
// native turn (terminal response event or a request-level error frame).
func wsNativeTerminal(data *spool) bool {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.NewDecoder(io.NewSectionReader(data.File, data.offset, data.size)).Decode(&probe); err != nil {
		return true // unparseable: stop attributing frames to this turn
	}
	switch probe.Type {
	case "response.completed", "response.incomplete", "response.failed", "error":
		return true
	}
	return false
}

// wsNativeClientFrame classifies and forwards one client frame.
func (k *keeper) wsNativeClientFrame(ctx context.Context, conn, upstream *websocket.Conn, r *http.Request, owner [32]byte, f *wsFrame, sess *wsSession, inflight **wsNativeInflight, nativeOpen *bool) (string, int, bool) {
	data := f.spool
	if f.typ != websocket.MessageText || !isWSCreate(data) {
		// Not a turn frame: relay verbatim, downstream owns the protocol.
		if err := k.wsRelayFrame(ctx, upstream, data, f.typ); err != nil {
			return "", http.StatusBadGateway, false
		}
		return "", http.StatusOK, false
	}
	var probe struct {
		Prev     string `json:"previous_response_id"`
		Generate *bool  `json:"generate"`
	}
	_ = json.NewDecoder(io.NewSectionReader(data.File, data.offset, data.size)).Decode(&probe)
	if probe.Generate != nil && !*probe.Generate && probe.Prev == "" {
		// Prewarm only makes sense as a downstream primitive; relay untouched
		// unless it chains onto a keeper-owned parent (handled below).
		if err := k.wsRelayFrame(ctx, upstream, data, f.typ); err != nil {
			return "", http.StatusBadGateway, false
		}
		*nativeOpen = true
		return "", http.StatusOK, false
	}
	if probe.Prev != "" {
		known, valid := k.store.ownedID(probe.Prev, owner)
		if !valid {
			if known || strings.HasPrefix(probe.Prev, keeperResponsePrefix) || probe.Prev == sess.lastID {
				return "", wsError(ctx, conn, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", code: "previous_response_not_found", param: "previous_response_id",
					message: fmt.Sprintf("Previous response with id '%s' not found.", probe.Prev)}), true
			}
			// Native parent: the delta is meaningful only downstream. Forward
			// verbatim and mark the turn so its events stay untouched.
			if err := k.wsRelayFrame(ctx, upstream, data, f.typ); err != nil {
				return "", http.StatusBadGateway, true
			}
			*nativeOpen = true
			return "", http.StatusOK, true
		}
	}
	// No parent, or a keeper-owned parent: build the upstream frame on disk
	// (expanding stored history for an owned parent) and record the turn.
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
		return "", wsError(ctx, conn, workingError(err)), probe.Prev != ""
	}
	t, apiErr := k.prepareDiskEnv(ctx, data, owner, lookup, true, true)
	leave()
	if apiErr != nil {
		return "", wsError(ctx, conn, apiErr), probe.Prev != ""
	}
	body, err := t.file.requestBody()
	if err != nil {
		t.file.Close()
		return "", wsError(ctx, conn, workingError(err)), probe.Prev != ""
	}
	w, err := upstream.Writer(ctx, websocket.MessageText)
	if err == nil {
		_, err = io.Copy(struct{ io.Writer }{w}, body)
	}
	closeErr := w.Close()
	body.Close()
	if err != nil || closeErr != nil {
		t.file.Close()
		return "", wsError(ctx, conn, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"}), probe.Prev != ""
	}
	*inflight = &wsNativeInflight{turn: t, owner: owner, sess: sess}
	return "", http.StatusOK, probe.Prev != ""
}

// wsNativeUpstreamFrame relays one upstream text frame through the collector,
// rewriting ids and store as local turns do. On the terminal frame it stores
// input+output, sets the session pin, and returns the stored public id; a
// non-zero status marks the turn finished (-1: connection-level failure).
func (k *keeper) wsNativeUpstreamFrame(ctx context.Context, conn *websocket.Conn, f *wsFrame, sess *wsSession, inflight *wsNativeInflight) int {
	data := f.spool
	raw := make([]byte, data.size)
	if _, err := data.ReadAt(raw, 0); err != nil {
		return -1
	}
	inflight.c.boundedEvent(raw, k.work)
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
		inflight.failed = true
		inflight.turn.file.Close()
		inflight.c.release(k.work)
		return -1
	}
	defer release()
	if err := conn.Write(ctx, websocket.MessageText, wire); err != nil {
		return -1
	}
	if terminal {
		inflight.turn.file.Close()
		inflight.c.release(k.work)
		return http.StatusOK
	}
	return 0
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

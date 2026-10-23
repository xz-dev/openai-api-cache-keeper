package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
)

// wsSession holds only bounded handles for one WebSocket socket: the id of
// its last accepted turn and a pin keeping that history continuable until
// replacement or disconnect, including store:false turns. No full history
// items are retained on an idle socket.
type wsSession struct {
	lastID  string // public id of the last completed turn
	lastPin string // opaque pin handle for the last accepted turn
	release func() // releases the store pin, if any
}

func (s *wsSession) clear() {
	if s.release != nil {
		s.release()
		s.release = nil
	}
	s.lastID, s.lastPin = "", ""
}

// serveWS runs the Responses WebSocket mode: each response.create frame is
// expanded like an HTTP request, sent upstream as streaming HTTP, and every
// upstream SSE event is relayed back as one text frame. Turns run in order.
func (k *keeper) serveWS(w http.ResponseWriter, r *http.Request) {
	if !trySlot(k.work.sockets) {
		writeAPIError(w, workingError(errors.New("socket limit")))
		return
	}
	defer func() { <-k.work.sockets }()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	conn.SetReadLimit(k.cfg.maxRequestBytes)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	header := r.Header.Clone()
	header.Del("Upgrade")
	owner := k.ownerOf(r)

	frames := make(chan *spool, 1)
	go func() {
		defer cancel()
		defer close(frames)
		for {
			typ, reader, err := conn.Reader(ctx)
			if err != nil {
				return
			}
			if typ != websocket.MessageText {
				if readDiscard(ctx, reader) != nil {
					return
				}
				continue
			}
			if !trySlot(k.work.turns) {
				if readDiscard(ctx, reader) != nil {
					return
				}
				wsError(ctx, conn, workingError(errors.New("turn limit")))
				continue
			}
			frame, err := k.spoolRequest(ctx, reader)
			if err != nil {
				<-k.work.turns
				wsError(ctx, conn, workingError(err))
				if readDiscard(ctx, reader) != nil {
					return
				}
				continue
			}
			select {
			case frames <- frame:
			case <-ctx.Done():
				frame.Close()
				<-k.work.turns
				return
			default:
				frame.Close()
				<-k.work.turns
				wsError(ctx, conn, workingError(errors.New("socket queue full")))
			}
		}
	}()

	sess := &wsSession{}
	defer sess.clear()
	for data := range frames {
		start := time.Now()
		id, status, hasPrev := k.wsTurn(ctx, conn, header, owner, data, sess)
		data.Close()
		<-k.work.turns
		logTurn("ws", status, hasPrev, id, start, k.store)
		if ctx.Err() != nil {
			break
		}
	}
	cancel()
	_ = conn.CloseNow()
	for data := range frames {
		data.Close()
		<-k.work.turns
	}
}

func (k *keeper) wsTurn(ctx context.Context, conn *websocket.Conn, header http.Header, owner [32]byte, data *spool, sess *wsSession) (string, int, bool) {
	upstream, _, routeErr := k.routeRequest(ctx, data, true)
	if routeErr != nil {
		return "", wsError(ctx, conn, routeErr), false
	}
	leave, err := k.work.enter(ctx)
	if err != nil {
		return "", wsError(ctx, conn, workingError(err)), false
	}
	t, apiErr := k.prepareDisk(ctx, data, owner, func(id string) ([32]byte, bool) {
		if id == sess.lastID && sess.lastPin != "" {
			if tip, _, ok := k.store.resolveID(sess.lastPin, owner, true); ok {
				return tip, true
			}
		}
		tip, _, ok := k.store.resolveID(id, owner, false)
		return tip, ok
	}, true)
	leave()
	data.Close()
	if apiErr != nil {
		return "", wsError(ctx, conn, apiErr), false
	}
	defer t.file.Close()
	hasPrev := t.hasPrev
	resp, err := k.forwardDisk(ctx, upstream, "/v1/responses", "", header, t)
	if err != nil {
		return "", wsError(ctx, conn, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"}), hasPrev
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		body, err := readResponseBody(resp.Body, k.work)
		if err != nil {
			return "", wsError(ctx, conn, responseError(err)), hasPrev
		}
		defer body.close()
		return "", k.writeUpstreamError(ctx, conn, resp.StatusCode, body.data), hasPrev
	}
	var c collector
	defer c.release(k.work)
	id := ""
	status := resp.StatusCode
	accept := func() (bool, error) {
		if rid, output, ok := c.final(); ok && id == "" {
			if err := ctx.Err(); err != nil {
				return false, err
			}
			pin, err := k.acceptDisk(ctx, t, publicID(rid), owner, output, sess.lastPin, true)
			if err != nil {
				// The previous pin stays intact; only this turn is not continuable.
				logNotRetained(err)
				return false, nil
			}
			// accept replaced the old pin atomically. Failure leaves it intact.
			id = publicID(rid)
			sess.lastID, sess.lastPin = id, pin
			sess.release = func() {
				if err := k.store.release(pin); err != nil {
					log.Printf("socket pin cleanup failed (records remain charged): %v", err)
				}
			}
		}
		return true, nil
	}
	err = readSSE(resp.Body, nil, func(event []byte) error {
		wire, release, err := k.responseFrame(ctx, &c, event, true, t.store, accept)
		if err != nil {
			return err
		}
		defer release()
		return conn.Write(ctx, websocket.MessageText, wire)
	}, k.work)
	if err == nil && !c.done && ctx.Err() == nil {
		status = wsFailure(ctx, conn, c.responseID(), c.nextSeq(), &apiError{status: 502, typ: "upstream_error", message: "upstream stream ended before terminal response"})
	}
	if err != nil {
		log.Printf("ws relay ended early: %v", err)
		if ctx.Err() == nil && status == http.StatusOK && id == "" {
			status = wsFailure(ctx, conn, c.responseID(), c.nextSeq(), responseError(err))
		}
	}
	return id, status, hasPrev
}

// wsFailure ends a started response with response.failed; otherwise it is a
// request-level error frame. The socket stays open for the next turn.
func wsFailure(ctx context.Context, conn *websocket.Conn, started string, seq int64, e *apiError) int {
	if started == "" {
		return wsError(ctx, conn, e)
	}
	frame, _ := json.Marshal(failureEvent(started, e, seq))
	_ = conn.Write(ctx, websocket.MessageText, frame)
	return e.status
}

func wsError(ctx context.Context, conn *websocket.Conn, e *apiError) int {
	frame, _ := json.Marshal(map[string]any{"type": "error", "status": e.status, "error": e.object()})
	_ = conn.Write(ctx, websocket.MessageText, frame)
	return e.status
}

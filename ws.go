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
	idFree  func() // owns the retained id independently of the turn collector
}

func (s *wsSession) reserveID(m *workingSet, id string) (func(), error) {
	if id == s.lastID {
		return nil, nil
	}
	return m.reserveInput(int64(len(id)))
}

func (s *wsSession) setID(id string, free func()) {
	if id == s.lastID {
		return
	}
	old := s.idFree
	s.lastID, s.idFree = id, free
	if old != nil {
		old()
	}
}

func (s *wsSession) clear() {
	if s.release != nil {
		s.release()
		s.release = nil
	}
	s.lastID, s.lastPin = "", ""
	if s.idFree != nil {
		s.idFree()
		s.idFree = nil
	}
}

// serveWS runs the Responses WebSocket mode: each response.create frame is
// expanded like an HTTP request, sent upstream as streaming HTTP, and every
// upstream SSE event is relayed back as one text frame. Turns run in order.
func (k *keeper) serveWS(w http.ResponseWriter, r *http.Request) {
	lease, err := k.work.state(socketStateBytes(r.Header), &k.work.activeSockets)
	if err != nil {
		writeAPIError(w, workingError(err))
		return
	}
	defer lease.Close()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	conn.SetReadLimit(k.cfg.maxRequestBytes + 1) // keeper, not the library, refuses (D5)
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
			frameState, stateErr := k.work.state(turnStateBytes(nil), &k.work.activeTurns)
			if stateErr != nil {
				if readDiscard(ctx, reader) != nil {
					return
				}
				wsError(ctx, conn, workingError(stateErr))
				continue
			}
			frame, err := k.spoolRequest(ctx, reader)
			if err != nil {
				frameState.Close()
				wsError(ctx, conn, workingError(err))
				if readDiscard(ctx, reader) != nil {
					return
				}
				continue
			}
			frame.frameState = frameState
			select {
			case frames <- frame:
			case <-ctx.Done():
				frame.Close()
				return
			default:
				frame.Close()
				wsError(ctx, conn, workingError(errors.New("socket queue full")))
			}
		}
	}()

	sess := &wsSession{}
	defer sess.clear()
	for data := range frames {
		workerState := data.frameState.retain()
		start := time.Now()
		id, status, hasPrev := k.wsTurn(ctx, conn, header, owner, data, sess)
		data.Close()
		workerState()
		logTurn("ws", status, hasPrev, id, start, k.store)
		if ctx.Err() != nil {
			break
		}
	}
	cancel()
	_ = conn.CloseNow()
	for data := range frames {
		data.Close()
	}
}

func (k *keeper) wsTurn(ctx context.Context, conn *websocket.Conn, header http.Header, owner [32]byte, data *spool, sess *wsSession) (string, int, bool) {
	upstream, _, routeErr := k.routeRequest(ctx, data, true)
	if routeErr != nil {
		return "", wsError(ctx, conn, routeErr), false
	}
	t, apiErr := k.prepareDiskSnapshot(ctx, data, owner, func(id string) (*historySnapshot, error) {
		if id == sess.lastID && sess.lastPin != "" {
			history, err := k.store.snapshotID(ctx, sess.lastPin, owner, true)
			if err != errMissingItem {
				return history, err
			}
		}
		return k.store.snapshotID(ctx, id, owner, false)
	}, true, false)
	data.Close()
	if apiErr != nil {
		return "", wsError(ctx, conn, apiErr), false
	}
	defer t.file.Close()
	hasPrev := t.hasPrev
	resp, err := k.forwardDisk(ctx, upstream, "/v1/responses", "", header, t)
	if err != nil {
		return "", wsError(ctx, conn, upstreamFailed(requestIDFrom(header), "ws forward", err)), hasPrev
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
	prepare := func() (*responseAcceptance, error) { return k.prepareSocketAcceptance(ctx, &c, t, owner, sess, &id) }
	err = readSSEOwned(resp.Body, nil, func(event *responseBuffer, suspend suspendResponseFrame) error {
		wire, release, _, err := k.responseFrameOwned(ctx, &c, event, true, t.store, prepare, suspend)
		if err != nil {
			return err
		}
		defer release()
		return conn.Write(ctx, websocket.MessageText, wire)
	}, k, k.work)
	if err == nil && !c.done && ctx.Err() == nil {
		status = wsFailure(ctx, conn, c.responseID(), c.nextSeq(), upstreamFailed(requestIDFrom(header), "ws relay", errors.New("upstream stream ended before terminal response")))
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

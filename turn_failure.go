package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// turnEnd records why a WS session ended while a turn was open, so the
// opaque 502 turn line has a matching cause line (side, close code, reason,
// request id). Log-only; it changes no status.
type turnEnd struct {
	side   string // "upstream", "client" or "keeper"
	code   websocket.StatusCode
	reason string
	cause  string
}

func endFromErr(side string, err error) turnEnd {
	e := turnEnd{side: side, code: -1}
	var ce websocket.CloseError
	switch {
	case errors.As(err, &ce):
		e.code, e.reason, e.cause = ce.Code, ce.Reason, "close frame"
	case errors.Is(err, errWorkSize):
		e.side, e.cause = "keeper", err.Error()
	case err != nil:
		e.cause = err.Error()
	default:
		e.cause = "socket ended"
	}
	return e
}

// requestIDOf returns the first request id an outer hop forwarded, or "-".
func requestIDOf(r *http.Request) string { return requestIDFrom(r.Header) }

func requestIDFrom(hdr http.Header) string {
	for _, h := range []string{"X-Request-Id", "X-Client-Request-Id", "X-Apisix-Request-Id"} {
		if v := hdr.Get(h); v != "" {
			if len(v) > 128 {
				v = v[:128]
			}
			return v
		}
	}
	return "-"
}

// then appends a later cause to the first one, which is never overwritten:
// the first side to fail is the one that explains a close (Q4).
func (e turnEnd) then(later turnEnd) turnEnd {
	e.cause = boundedText(fmt.Sprintf("%s; then side=%s close_code=%d reason=%q cause=%q", e.cause, later.side, later.code, boundedText(later.reason), boundedText(later.cause)))
	return e
}

// wsClock is a socket's age and the time its last frame arrived from each
// side, so a close can be matched to idle or keepalive timers (Q4).
type wsClock struct {
	start                 time.Time
	lastClient, lastInner atomic.Int64 // UnixNano of the last frame read; 0 = none
}

func newWSClock() *wsClock { return &wsClock{start: time.Now()} }

func since(nanos int64, now time.Time) string {
	if nanos == 0 {
		return "none"
	}
	return now.Sub(time.Unix(0, nanos)).Round(time.Millisecond).String()
}

func (c *wsClock) String() string {
	now := time.Now()
	return fmt.Sprintf("age=%s since_client_frame=%s since_upstream_frame=%s",
		now.Sub(c.start).Round(time.Millisecond), since(c.lastClient.Load(), now), since(c.lastInner.Load(), now))
}

func logTurnEnd(e turnEnd, requestID, storedID string, clock *wsClock) {
	log.Printf("ws turn ended early: side=%s close_code=%d reason=%q cause=%q request_id=%s response=%s %s",
		e.side, e.code, boundedText(e.reason), boundedText(e.cause), requestID, orDash(storedID), clock)
}

// logSessionEnd records the close cause of a socket with no open turn, so
// every WS cleanup has a cause line.
func logSessionEnd(e turnEnd, requestID string, clock *wsClock) {
	log.Printf("ws session closed: side=%s close_code=%d reason=%q cause=%q request_id=%s %s",
		e.side, e.code, boundedText(e.reason), boundedText(e.cause), requestID, clock)
}

// ensureRequestID gives a request without an outer request id a keeper one,
// sent on to the inner hop, so both sides' logs carry the same id (Q4).
func ensureRequestID(r *http.Request) {
	if requestIDOf(r) == "-" {
		r.Header.Set("X-Request-Id", "keeper-"+rand.Text())
	}
}

func boundedText(s string) string {
	if len(s) > 256 {
		return s[:256]
	}
	return s
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// innerEndError is the client-visible error for an upstream socket that
// ended while a turn was open: never a bare close (design D7).
func innerEndError(e turnEnd) *apiError {
	switch {
	case e.side == "keeper":
		return &apiError{status: http.StatusBadGateway, typ: "upstream_error", code: "upstream_frame_too_large",
			message: "keeper: an upstream frame was refused and the turn was ended: " + boundedText(e.cause)}
	case e.code == websocket.StatusMessageTooBig:
		return &apiError{status: http.StatusRequestEntityTooLarge, typ: "invalid_request_error", code: "request_too_large", hop: keeperHop,
			message: fmt.Sprintf("keeper: the inner hop closed the WebSocket with 1009 (message too big): %s", boundedText(e.reason))}
	case e.code >= 0:
		return &apiError{status: http.StatusBadGateway, typ: "upstream_error", code: "upstream_closed",
			message: fmt.Sprintf("keeper: the inner hop closed the WebSocket during the turn (code %d): %s", e.code, boundedText(e.reason))}
	}
	return &apiError{status: http.StatusBadGateway, typ: "upstream_error", code: "upstream_failed",
		message: "keeper: the inner hop connection failed during the turn: " + boundedText(e.cause)}
}

// failOpenTurn ends an open turn in protocol terms before the client socket
// closes. ctx must outlive the handler's cancelled pump context.
func failOpenTurn(ctx context.Context, conn *websocket.Conn, inflight *wsNativeInflight, e turnEnd) {
	started, seq := "", int64(0)
	if inflight != nil {
		started, seq = inflight.c.responseID(), inflight.c.nextSeq()
	}
	wsFailure(ctx, conn, started, seq, innerEndError(e))
}

// upstreamFailed logs a keeper 502 with its cause (side, close code,
// reason, request id, site) and returns the client-visible error naming
// keeper and the cause; it never leaves the client with a bare failure.
func upstreamFailed(requestID, where string, err error) *apiError {
	e := endFromErr("upstream", err)
	log.Printf("keeper 502: side=%s close_code=%d reason=%q cause=%q request_id=%s where=%s",
		e.side, e.code, boundedText(e.reason), boundedText(e.cause), requestID, where)
	return &apiError{status: http.StatusBadGateway, typ: "upstream_error", code: "upstream_failed",
		message: fmt.Sprintf("keeper: the inner hop request failed (%s): %s", where, boundedText(e.cause))}
}

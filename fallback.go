package main

import (
	"fmt"
	"io"
	"net/http"
	"strings"
)

// fallbackLimit is L (design D2): the largest body a fallback path carries.
// Every path that is not a covered Responses endpoint is a fallback path,
// and keeper, the first hop, refuses above L with the same named 413 the
// downstream hop sends (Sub2API's unconverted-route 413).
const fallbackLimit int64 = 16 << 20

// keeperHop names keeper in the hop field of its named size refusals.
const keeperHop = "keeper"

// fallbackExempt reports the routes keeper leaves without a body limit:
// Sub2API's admin and management API (account and proxy data imports,
// plugin upload), which can legitimately carry bodies above L. The limit for
// them is the user's decision (review M3); until then keeper keeps today's
// behaviour and Sub2API's own limits apply.
func fallbackExempt(path string) bool {
	return strings.HasPrefix(path, "/api/v1/admin/")
}

// fallbackTooLarge is the named 413 of a body bounded by L; size < 0 when
// the body had no Content-Length and was counted as it arrived. what names
// the request (see boundedWhat).
func fallbackTooLarge(what string, size int64) *apiError {
	msg := fmt.Sprintf("%s: %s; its limit is %d bytes", keeperHop, what, fallbackLimit)
	if size >= 0 {
		msg += fmt.Sprintf(" and the body has %d", size)
	}
	return &apiError{status: http.StatusRequestEntityTooLarge, typ: "invalid_request_error", code: "request_too_large", hop: keeperHop, limit: fallbackLimit, message: msg}
}

// gatedRequest reports the only requests the D4 gate admits and tickets: a
// POST without Upgrade or a GET with Upgrade on a covered path. Every other
// request, on any path, is bounded by L or refused (review 2026-11-05 M1).
func gatedRequest(r *http.Request) bool {
	if !coveredPath(r.URL.Path) {
		return false
	}
	up := isUpgradeRequest(r)
	return (r.Method == http.MethodPost && !up) || (r.Method == http.MethodGet && up)
}

// boundedWhat names a request bounded by L in its 413 message.
func boundedWhat(r *http.Request) string {
	if coveredPath(r.URL.Path) {
		return fmt.Sprintf("%s %s is not a gated Responses request", r.Method, r.URL.Path)
	}
	return r.URL.Path + " is a fallback path"
}

// unboundedUpgrade refuses an Upgrade request that is not a GET. Keeper
// relays an upgraded socket frame by frame only for a WebSocket handshake,
// which is always a GET (RFC 6455); any other upgraded connection would be
// an opaque byte tunnel that L cannot bound.
func unboundedUpgrade(r *http.Request) *apiError {
	return &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", code: "upgrade_not_bounded", hop: keeperHop,
		message: fmt.Sprintf("%s: %s %s with Upgrade cannot be bounded; an upgraded connection must be a GET WebSocket handshake", keeperHop, r.Method, r.URL.Path)}
}

// boundFallback applies L to every request with a body that the gate does
// not admit, for every method and path: a fallback path, and a covered path
// with any method/Upgrade combination other than gatedRequest. A declared
// length above L is refused here, before any upstream call, spool write,
// ticket or billing call; it returns false after writing the 413. A body
// without a length is counted as it streams: past L the read fails with
// fallbackOverLimit and the native proxy answers the same 413. A non-GET
// Upgrade request is refused (unboundedUpgrade). Only the admin routes are
// exempt.
func (k *keeper) boundFallback(w http.ResponseWriter, r *http.Request) bool {
	if fallbackExempt(r.URL.Path) {
		return true
	}
	if isUpgradeRequest(r) && r.Method != http.MethodGet {
		writeAPIError(w, unboundedUpgrade(r))
		return false
	}
	if gatedRequest(r) || r.Body == nil || r.Body == http.NoBody {
		return true
	}
	if r.ContentLength > fallbackLimit {
		writeAPIError(w, fallbackTooLarge(boundedWhat(r), r.ContentLength))
		return false
	}
	if r.ContentLength < 0 {
		r.Body = &fallbackBody{ReadCloser: r.Body, what: boundedWhat(r)}
	}
	return true
}

// fallbackOverLimit fails the read of a counted fallback body past L.
type fallbackOverLimit struct{ what string }

func (e *fallbackOverLimit) Error() string {
	return fallbackTooLarge(e.what, -1).message
}

// fallbackBody counts a body without a declared length; it delivers at
// most L bytes and fails the read that would cross L.
type fallbackBody struct {
	io.ReadCloser
	what string
	n    int64
}

func (b *fallbackBody) Read(p []byte) (int, error) {
	if b.n >= fallbackLimit {
		// One probe byte tells a body of exactly L from a longer one.
		var one [1]byte
		m, err := b.ReadCloser.Read(one[:])
		if m > 0 {
			return 0, &fallbackOverLimit{b.what}
		}
		return 0, err
	}
	if rest := fallbackLimit - b.n; int64(len(p)) > rest {
		p = p[:rest]
	}
	m, err := b.ReadCloser.Read(p)
	b.n += int64(m)
	return m, err
}

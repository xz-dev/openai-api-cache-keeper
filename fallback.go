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

// fallbackTooLarge is the named 413 of a fallback path; size < 0 when the
// body had no Content-Length and was counted as it arrived.
func fallbackTooLarge(path string, size int64) *apiError {
	msg := fmt.Sprintf("%s: %s is a fallback path; its limit is %d bytes", keeperHop, path, fallbackLimit)
	if size >= 0 {
		msg += fmt.Sprintf(" and the body has %d", size)
	}
	return &apiError{status: http.StatusRequestEntityTooLarge, typ: "invalid_request_error", code: "request_too_large", hop: keeperHop, message: msg}
}

// boundFallback applies L to a request with a body on a fallback path. A
// declared length above L is refused here, before any upstream call, spool
// write, ticket or billing call; it returns false after writing the 413. A
// body without a length is counted as it streams: past L the read fails
// with fallbackOverLimit and the native proxy answers the same 413.
func (k *keeper) boundFallback(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost || coveredPath(r.URL.Path) || fallbackExempt(r.URL.Path) ||
		r.Body == nil || r.Body == http.NoBody {
		return true
	}
	if r.ContentLength > fallbackLimit {
		writeAPIError(w, fallbackTooLarge(r.URL.Path, r.ContentLength))
		return false
	}
	if r.ContentLength < 0 {
		r.Body = &fallbackBody{ReadCloser: r.Body, path: r.URL.Path}
	}
	return true
}

// fallbackOverLimit fails the read of a counted fallback body past L.
type fallbackOverLimit struct{ path string }

func (e *fallbackOverLimit) Error() string {
	return fallbackTooLarge(e.path, -1).message
}

// fallbackBody counts a body without a declared length; it delivers at
// most L bytes and fails the read that would cross L.
type fallbackBody struct {
	io.ReadCloser
	path string
	n    int64
}

func (b *fallbackBody) Read(p []byte) (int, error) {
	if b.n >= fallbackLimit {
		// One probe byte tells a body of exactly L from a longer one.
		var one [1]byte
		m, err := b.ReadCloser.Read(one[:])
		if m > 0 {
			return 0, &fallbackOverLimit{b.path}
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

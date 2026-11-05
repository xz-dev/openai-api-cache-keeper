package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// largeBodyBytes is the ticket threshold (design D4): a body or expanded
// history of at least this size is admitted by the host gate, and its
// caller is authenticated first.
const largeBodyBytes int64 = 64 << 20

// authRefusal carries the client-visible result of a failed or unavailable
// authentication through spoolRequest.
type authRefusal struct{ e *apiError }

func (a *authRefusal) Error() string { return a.e.message }

// largeAuth authenticates a large-body caller with the request's own
// credentials. nil means authentication is not required at this site.
type largeAuth func(ctx context.Context) error

// largeBodyAuth returns the authentication step for requests carrying hdr
// and rawQuery: the existing Sub2API billing revalidation, failing closed.
// 401/403 refuse the caller; any other failure is 503 auth_unavailable.
func (k *keeper) largeBodyAuth(hdr http.Header, rawQuery string) largeAuth {
	return func(ctx context.Context) error {
		if k.cfg.upstream == nil {
			return &authRefusal{authUnavailable("no billing revalidation is configured in routed mode")}
		}
		target := *k.cfg.upstream
		target.Path = strings.TrimRight(target.Path, "/") + "/v1/sub2api/billing"
		target.RawQuery = rawQuery
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
		if err != nil {
			return &authRefusal{authUnavailable(err.Error())}
		}
		copyHeader(req.Header, hdr)
		stripUntrustedForwarding(req.Header)
		resp, err := k.client.Do(req)
		if err != nil {
			return &authRefusal{authUnavailable(err.Error())}
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		resp.Body.Close()
		switch {
		case resp.StatusCode >= 200 && resp.StatusCode < 300:
			return nil
		case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden:
			return &authRefusal{authFailed(resp.StatusCode)}
		}
		return &authRefusal{authUnavailable(fmt.Sprintf("billing revalidation returned status %d", resp.StatusCode))}
	}
}

// authFailed refuses a large body whose caller the billing check rejected.
func authFailed(status int) *apiError {
	return &apiError{status: status, typ: "invalid_request_error", code: "invalid_api_key", hop: keeperHop,
		message: fmt.Sprintf("keeper: authentication failed (status %d); a body of %d bytes or more is admitted only for an authenticated caller", status, largeBodyBytes)}
}

func authUnavailable(cause string) *apiError {
	return &apiError{status: http.StatusServiceUnavailable, typ: "server_error", code: "auth_unavailable", hop: keeperHop,
		message: "keeper: caller authentication is unavailable, so the large request was not admitted: " + boundedText(cause)}
}

// copyAuthenticated copies src to dst, running auth once the copied bytes
// reach largeBodyBytes and before any byte beyond them is read.
func copyAuthenticated(ctx context.Context, dst io.Writer, src io.Reader, limit int64, auth largeAuth, buf []byte) (int64, error) {
	if auth == nil || limit < largeBodyBytes {
		return io.CopyBuffer(dst, io.LimitReader(src, limit), buf)
	}
	n, err := io.CopyBuffer(dst, io.LimitReader(src, largeBodyBytes), buf)
	if err != nil || n < largeBodyBytes {
		return n, err
	}
	if err := auth(ctx); err != nil {
		return n, err
	}
	m, err := io.CopyBuffer(dst, io.LimitReader(src, limit-n), buf)
	return n + m, err
}

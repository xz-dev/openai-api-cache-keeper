package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
)

// TestRefusalCarriesHopAndLimit checks that every keeper size or capacity
// refusal carries code, hop "keeper" and, where a byte limit applies,
// limit_bytes (design D9), both in the HTTP error body and in the WS error
// and response.failed events. Status codes are the unchanged ones.
func TestRefusalCarriesHopAndLimit(t *testing.T) {
	cases := []struct {
		name   string
		e      *apiError
		status int
		code   string
		limit  int64  // 0 = no limit_bytes
		hop    string // "" = keeperHop
	}{
		{"gate capacity_busy", refusal(10, 5, 100, 100, 95, 0), 503, "capacity_busy", 10, ""},
		{"gate capacity_busy memory", refusal(10, 50, 100, 100, 0, 60), 503, "capacity_busy", 50, ""},
		{"gate insufficient_storage", refusal(200, 5, 100, 100, 0, 0), 507, "insufficient_storage", 200, ""},
		{"gate insufficient_memory", refusal(10, 200, 100, 100, 0, 0), 503, "insufficient_memory", 200, ""},
		{"ticket resize after spool", workingError(&authRefusal{refusal(200, 5, 100, 100, 0, 0)}), 507, "insufficient_storage", 200, ""},
		{"ticket ledger unavailable", ledgerUnavailable(errors.New("flock failed")), 503, "capacity_ledger_unavailable", 0, ""},
		{"spool over MAX_REQUEST_BYTES", workingError(requestSizeError(1234)), 413, "request_too_large", 1234, ""},
		{"http body over MAX_REQUEST_BYTES", workingError(&http.MaxBytesError{Limit: 99}), 413, "request_too_large", 99, ""},
		{"history cap", workingError(errHistorySize), 413, "history_too_large", maxHistoryBytes, ""},
		{"spool disk floor", workingError(&storageFloorError{free: 1, need: 2}), 507, "insufficient_storage", spoolDiskFloor, ""},
		{"cgroup memory charge", workingError(&insufficientMemoryError{need: 77, free: 1}), 503, "insufficient_memory", 77, ""},
		{"working set too large", workingError(errWorkSize), 413, "working_set_too_large", 0, ""},
		{"working set unavailable", workingError(errors.New("socket queue full")), 503, "working_set_unavailable", 0, ""},
		{"native socket limit", errNativeSocketLimit, 503, "working_set_unavailable", 0, ""},
		{"fallback L declared", fallbackTooLarge("/v1/chat/completions", fallbackLimit+1), 413, "request_too_large", fallbackLimit, ""},
		{"fallback L counted", fallbackTooLarge("/v1/chat/completions", -1), 413, "request_too_large", fallbackLimit, ""},
		// The inner hop refused with 1009: the hop is the upstream, not keeper.
		{"inner WS 1009", innerEndError(turnEnd{side: "upstream", code: websocket.StatusMessageTooBig, reason: "too big"}), 413, "request_too_large", 0, upstreamHop},
		{"upstream frame refused", innerEndError(turnEnd{side: "keeper", code: -1, cause: "upstream frame refused"}), 502, "upstream_frame_too_large", 0, ""},
		{"large auth failed", authFailed(401), 401, "invalid_api_key", 0, ""},
		{"large auth unavailable", authUnavailable("down"), 503, "auth_unavailable", 0, ""},
		{"upstream response malformed", responseError(errUpstreamData), 502, "upstream_response_malformed", 0, ""},
		{"upstream output_index", responseError(&outputIndexError{index: maxOutputIndex + 1}), 502, "upstream_output_index_too_large", maxCollectedBytes, ""},
		{"covered path ungated L", fallbackTooLarge("PUT /v1/responses is not a gated Responses request", fallbackLimit+1), 413, "request_too_large", fallbackLimit, ""},
		{"non-GET upgrade", unboundedUpgrade(httptest.NewRequest(http.MethodPost, "/v1/responses", nil)), 400, "upgrade_not_bounded", 0, ""},
		{"client frame unparseable", (&clientFrameError{errors.New("x")}).apiError(), 400, "client_frame_unparseable", 0, ""},
	}
	check := func(t *testing.T, where string, obj map[string]any, code string, limit int64, hop string) {
		t.Helper()
		if hop == "" {
			hop = keeperHop
		}
		if obj["code"] != code || obj["hop"] != hop {
			t.Fatalf("%s: code/hop %v", where, obj)
		}
		got, ok := obj["limit_bytes"]
		if limit == 0 {
			if ok {
				t.Fatalf("%s: unexpected limit_bytes %v", where, obj)
			}
			return
		}
		if got != float64(limit) {
			t.Fatalf("%s: limit_bytes %v, want %d", where, got, limit)
		}
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.e.status != c.status {
				t.Fatalf("status %d, want %d", c.e.status, c.status)
			}
			rec := httptest.NewRecorder()
			writeAPIError(rec, c.e)
			var body struct {
				Error map[string]any `json:"error"`
			}
			if rec.Code != c.status || json.Unmarshal(rec.Body.Bytes(), &body) != nil {
				t.Fatalf("HTTP %d %s", rec.Code, rec.Body)
			}
			check(t, "HTTP", body.Error, c.code, c.limit, c.hop)

			for _, started := range []string{"", "resp_x"} {
				b, _ := json.Marshal(failureEvent(started, c.e, 3))
				var ev struct {
					Type     string         `json:"type"`
					Error    map[string]any `json:"error"`
					Response struct {
						Error map[string]any `json:"error"`
					} `json:"response"`
				}
				if json.Unmarshal(b, &ev) != nil {
					t.Fatalf("WS event %s", b)
				}
				obj := ev.Error
				if started != "" {
					obj = ev.Response.Error
				}
				check(t, "WS "+ev.Type, obj, c.code, c.limit, c.hop)
			}
		})
	}
}

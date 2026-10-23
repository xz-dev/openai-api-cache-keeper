package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Regression tests ported from the third bounded review
// (/var/tmp/keeper-review3-20261022). Each maps to a REVIEW3 blocker.

// REVIEW3-1: all events of a response the keeper could not retain must carry
// one stable id; only retained turns may publish the resp_ alias.
func TestReview4UnretainedResponseIDConsistent(t *testing.T) {
	_, srv, k := newWSNativeKeeper(t, func(c *config) { c.maxStoreBytes = 1; c.storePinBytes = 1 })
	conn := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if e := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"A"}`)); e != nil {
		t.Fatal(e)
	}
	var ids []string
	for {
		_, b, e := conn.Read(ctx)
		if e != nil {
			t.Fatal(e)
		}
		var v struct {
			Type     string `json:"type"`
			Response struct {
				ID string `json:"id"`
			} `json:"response"`
		}
		if e := json.Unmarshal(b, &v); e != nil {
			t.Fatal(e)
		}
		if v.Response.ID != "" {
			ids = append(ids, v.Response.ID)
		}
		if v.Type == "response.completed" {
			break
		}
	}
	if n, _ := k.store.stats(); n != 0 {
		t.Fatal("fixture stored history", n)
	}
	if len(ids) < 2 {
		t.Fatal("fixture expected created and terminal ids", ids)
	}
	for _, id := range ids {
		if id != ids[0] {
			t.Fatalf("response id changed within unretained response: %q", ids)
		}
	}
}

// REVIEW3-2: WHATWG §9.2.6 — every data field appends its value plus one LF;
// dispatch strips exactly the last LF. Empty data fields are real fields.
func TestReview4SSEEmptyDataLines(t *testing.T) {
	tests := []struct{ stream, want string }{
		{"data:\n\n", ""},
		{"data:\ndata: X\n\n", "\nX"},
		{"data: X\ndata:\n\n", "X\n"},
		{"data\ndata: X\n\n", "\nX"},
		{"data: X\ndata:\ndata: Y\n\n", "X\n\nY"},
		{"data: \n\n", ""},          // "data: " with stripped space → empty
		{"event: e\ndata: X\n\n", "X"}, // non-data fields still coexist
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var got []string
			if e := readSSE(strings.NewReader(tc.stream), nil, func(p []byte) error {
				got = append(got, string(p))
				return nil
			}); e != nil {
				t.Fatal(e)
			}
			if len(got) != 1 || got[0] != tc.want {
				t.Fatalf("WHATWG event data: want [%q], got %q", tc.want, got)
			}
		})
	}
}

// REVIEW3-2 raw: an event containing only empty data fields still forwards
// its raw frame bytes at the dispatch boundary.
func TestReview4SSEEmptyDataRawFrame(t *testing.T) {
	stream := "data:\ndata:\n\n"
	var frames []string
	e := readSSE(strings.NewReader(stream), func(f []byte) error {
		frames = append(frames, string(f))
		return nil
	}, nil)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Join(frames, "") != stream {
		t.Fatalf("empty-data event frame not forwarded: %q", frames)
	}
}

// REVIEW3-3: a locally generated response.failed continues the upstream
// sequence_number and stays inside the ResponseFailedEvent schema.
func TestReview4GeneratedFailedEventSequence(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: {\"type\":\"response.created\",\"sequence_number\":7,\"response\":{\"id\":\"resp_started\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
		// EOF without terminal forces the keeper failureEvent branch.
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	_, body, _ := post(t, srv, "Bearer a", `{"model":"m","stream":true,"input":[]}`)
	var failure map[string]json.RawMessage
	e := readSSE(strings.NewReader(body), nil, func(raw []byte) error {
		var obj map[string]json.RawMessage
		if e := json.Unmarshal(raw, &obj); e != nil {
			return e
		}
		if string(obj["type"]) == `"response.failed"` {
			failure = obj
		}
		return nil
	})
	if e != nil {
		t.Fatal(e)
	}
	if failure == nil {
		t.Fatal("missing failure event", body)
	}
	var seq int64
	if e := json.Unmarshal(failure["sequence_number"], &seq); e != nil || seq != 8 {
		t.Fatalf("generated response.failed must continue upstream sequence (want 8): %s", failure)
	}
}

// REVIEW3-4: only the known response-bearing lifecycle event types get the
// id rewrite; any other response.* extension relays byte-for-byte.
func TestReview4UnknownResponseExtensionUnchanged(t *testing.T) {
	_, _, k := newWSNativeKeeper(t)
	raw := []byte(`{ "type":"response.vendor_telemetry", "response":{"id":"trace_7","status":"active"}, "sequence_number":42}`)
	var c collector
	defer c.release(k.work)
	wire, release, e := k.responseFrame(context.Background(), &c, raw, true, true, nil)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if !bytes.Equal(raw, wire) {
		t.Fatalf("unknown response.* event rewritten: %s", wire)
	}
}

// REVIEW3-4b: each known response-bearing event type still gets rewritten.
func TestReview4KnownResponseEventsStillRewritten(t *testing.T) {
	_, _, k := newWSNativeKeeper(t)
	for _, kind := range []string{"response.created", "response.in_progress", "response.queued", "response.completed", "response.incomplete", "response.failed"} {
		var status string
		switch kind {
		case "response.completed":
			status = "completed"
		case "response.incomplete":
			status = "incomplete"
		case "response.failed":
			status = "failed"
		case "response.queued":
			status = "queued"
		default:
			status = "in_progress"
		}
		var c collector
		raw := []byte(fmt.Sprintf(`{"type":%q,"response":{"id":"wsid_x","object":"response","status":%q,"output":[]}}`, kind, status))
		wire, release, e := k.responseFrame(context.Background(), &c, raw, true, true, nil)
		if e != nil {
			t.Fatalf("%s: %v", kind, e)
		}
		if !strings.Contains(string(wire), `"id":"resp_wsid_x"`) {
			t.Fatalf("%s response id not rewritten: %s", kind, wire)
		}
		release()
		c.release(k.work)
	}
}

// REVIEW3-5: a mid-capture spool failure must produce an explicit error
// body/status, never a silent partial or empty replay of the rejection.
func TestReview4HandshakeSpoolFailureExplicit(t *testing.T) {
	body := strings.Repeat("rejection-", 512)
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(403)
		io.WriteString(w, body)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	k.work.mu.Lock()
	k.work.disk = maxSpoolBytes - 200
	k.work.mu.Unlock()
	defer func() {
		k.work.mu.Lock()
		k.work.disk = 0
		k.work.mu.Unlock()
	}()
	req := httptest.NewRequest("GET", "/v1/responses", nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	rec := httptest.NewRecorder()
	k.ServeHTTP(rec, req)
	if rec.Code == 403 && rec.Body.String() != body {
		t.Fatalf("capture failure silently returns partial upstream rejection: status=%d length=%d want=%d", rec.Code, rec.Body.Len(), len(body))
	}
	if rec.Code == 403 && rec.Body.Len() == 0 {
		t.Fatalf("capture failure returned empty body with upstream status")
	}
}

// REVIEW3-6: full-request preparation must stream file spans instead of
// decoding and re-encoding a multi-megabyte input value.
func TestReview4PreparedInputMaterialisation(t *testing.T) {
	_, _, k := newWSNativeKeeper(t, func(c *config) { c.maxRequestBytes = 8 << 20 })
	raw, e := k.spoolRequest(context.Background(), strings.NewReader(`{"type":"response.create","input":"`+strings.Repeat("x", 4<<20)+`"}`))
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	turn, apiErr := k.prepareDiskEnv(context.Background(), raw, [32]byte{}, func(string) ([32]byte, bool) { return [32]byte{}, false }, true, true)
	runtime.ReadMemStats(&b)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	defer turn.file.Close()
	allocated := b.TotalAlloc - a.TotalAlloc
	t.Logf("4MiB input preparation allocated %d bytes; upstream spool=%d bytes", allocated, turn.file.size)
	if allocated > 2<<20 {
		t.Fatalf("preparation re-materialises a complete 4MiB value instead of streaming file spans: allocated %d bytes", allocated)
	}
}

// REVIEW3-6b: the upstream spool built by preparation still produces the
// same wire body for a string input (wrapped as a user item).
func TestReview4PreparedStringInputBody(t *testing.T) {
	_, _, k := newWSNativeKeeper(t)
	raw, e := k.spoolRequest(context.Background(), strings.NewReader(`{"type":"response.create","model":"m","input":"hi"}`))
	if e != nil {
		t.Fatal(e)
	}
	defer raw.Close()
	turn, apiErr := k.prepareDiskEnv(context.Background(), raw, [32]byte{}, func(string) ([32]byte, bool) { return [32]byte{}, false }, true, true)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	defer turn.file.Close()
	body, e := turn.file.requestBody()
	if e != nil {
		t.Fatal(e)
	}
	defer body.Close()
	got, _ := io.ReadAll(body)
	if !strings.Contains(string(got), `"content":"hi"`) || !strings.Contains(string(got), `"role":"user"`) {
		t.Fatalf("string input not wrapped as user item: %s", got)
	}
}

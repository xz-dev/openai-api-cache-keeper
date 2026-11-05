package main

import (
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
)

func TestFinalOutputMetadataBeforeAllocation(t *testing.T) {
	m := newWorkingSet()
	var c collector
	raw := []byte(`{"id":"resp_many","status":"completed","output":[` + strings.Repeat(`{},`, 200000-1) + `{}]}`)
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	err := c.boundedResponse(raw, m)
	runtime.ReadMemStats(&b)
	var live runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&live)
	t.Logf("source=%d outputItems=%d charged=%d totalAllocDelta=%d heapAllocDelta=%d err=%v", len(raw), len(c.output), m.output, b.TotalAlloc-a.TotalAlloc, int64(b.HeapAlloc)-int64(a.HeapAlloc), err)
	// No item count cap (D5): 200,000 items are accepted, and every header
	// slot is charged before it is allocated.
	if err != nil || len(c.output) != 200000 {
		t.Fatalf("collection refused: %v / %d", err, len(c.output))
	}
	// The reservation held while collecting covers everything allocated,
	// and the charge kept afterwards covers what stays live.
	if alloc := int64(b.TotalAlloc - a.TotalAlloc); m.outputPeak < alloc {
		t.Errorf("peak reservation %d < allocated %d", m.outputPeak, alloc)
	}
	if kept := int64(live.HeapAlloc) - int64(a.HeapAlloc); m.output < kept {
		t.Errorf("held charge %d < live heap %d", m.output, kept)
	}
	c.release(m)
}

func TestFinalMalformedJSONExplicit(t *testing.T) {
	for _, body := range []string{`{"id":"resp_bad","status":"completed","output":[`, `{"object":"response","status":"completed","output":[]}`} {
		t.Run(fmt.Sprint(len(body)), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, body)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newLocalTestKeeper(t, func(c *config) { c.upstream = u })
			status, got, _ := post(t, srv, "Bearer test", `{"input":"A"}`)
			entries, _ := k.store.stats()
			t.Logf("status=%d entries=%d body=%s", status, entries, got)
			if status == 200 {
				t.Error("malformed/invalid-terminal successful body not explicitly rejected")
			}
		})
	}
}

type finalEarlyTransport struct {
	resume <-chan struct{}
	result chan error
}

func (f finalEarlyTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	go func() { <-f.resume; _, err := io.Copy(io.Discard, r.Body); r.Body.Close(); f.result <- err }()
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"object":"response","id":"resp_early","status":"completed","output":[]}`))}, nil
}
func TestFinalEarlyResponseBodyLifetime(t *testing.T) {
	_, _, k := newLocalTestKeeper(t)
	resume := make(chan struct{})
	result := make(chan error, 1)
	k.client = &http.Client{Transport: finalEarlyTransport{resume, result}}
	req := httptest.NewRequest("POST", "http://keeper/v1/responses", strings.NewReader(`{"input":"still reading"}`))
	out := httptest.NewRecorder()
	k.serveLocalTurn(out, req)
	close(resume)
	err := <-result
	t.Logf("clientStatus=%d transportReadError=%v", out.Code, err)
	if err != nil {
		t.Errorf("request spool destroyed before transport closed request body: %v", err)
	}
}

func TestFinalEscapedOutputRewrite(t *testing.T) {
	m := newWorkingSet()
	var c collector
	raw := []byte(`{"object":"response","id":"msg_x","status":"completed","output":[{"type":"message","text":"` + strings.Repeat("<", maxEventBytes/2) + `"}]}`)
	if err := c.boundedEvent(append(append([]byte(`{"type":"response.completed","response":`), raw...), '}'), m); err != nil {
		t.Fatal(err)
	}
	out := rewriteResponse(raw, true)
	t.Logf("raw=%d rewritten=%d charge=%d", len(raw), len(out), m.output)
	if int64(len(out)) > m.output {
		t.Error("rewritten output allocation not covered by output reservation")
	}
}

func TestFinalIncompleteTransportDoesNotCommit(t *testing.T) {
	_, _, k := newLocalTestKeeper(t)
	// A complete JSON token followed by truncated HTTP framing is not a complete SSE event.
	raw := `data: {"type":"response.completed","response":{"id":"resp_truncated","status":"completed","output":[]}}`
	k.client = &http.Client{Transport: finalErrorTransport{raw}}
	out := httptest.NewRecorder()
	k.serveLocalTurn(out, httptest.NewRequest("POST", "http://keeper/v1/responses", strings.NewReader(`{"input":"A","stream":true}`)))
	n, _ := k.store.stats()
	t.Logf("entries=%d body=%s", n, out.Body.String())
	if n != 0 || strings.Contains(out.Body.String(), "response.completed") {
		t.Error("truncated event committed and relayed as terminal success")
	}
}

type finalErrorTransport struct{ raw string }

func (f finalErrorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	io.Copy(io.Discard, r.Body)
	r.Body.Close()
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(io.MultiReader(strings.NewReader(f.raw), finalReadError{}))}, nil
}

type finalReadError struct{}

func (finalReadError) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }

var _ = context.Background
var _ = json.RawMessage{}

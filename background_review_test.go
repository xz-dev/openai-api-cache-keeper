package main

// Regressions for defects reproduced by an independent review's probes.

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// delayedHeader parks the creator's WriteHeader until generation (including
// failure cleanup) has finished, a legal scheduler interleaving.
type delayedHeader struct {
	http.ResponseWriter
	k *keeper
}

func (w delayedHeader) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w delayedHeader) WriteHeader(status int) {
	if status == http.StatusOK {
		w.k.cacheMu.Lock()
		var task *responseTask
		for _, candidate := range w.k.tasks {
			task = candidate
		}
		w.k.cacheMu.Unlock()
		if task != nil {
			<-task.finished
		}
	}
	w.ResponseWriter.WriteHeader(status)
}

// A creator that follows only after the generation failed still receives the
// recorded prefix (with the task id) and then an abort, never a clean end.
func TestBackgroundFailedPrefixBeforeFollow(t *testing.T) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 1, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","del`)
	})
	_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		k.ServeHTTP(delayedHeader{w, k}, r)
	}))
	defer srv.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"input":"A","background":true,"stream":true}`))
	req.Header.Set("Authorization", "Bearer owner")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("recorded prefix lost before follow: %v", err)
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(resp.Body)
	if !bytes.Contains(body, []byte("response.created")) || readErr == nil {
		t.Fatalf("failed creator: body=%q readErr=%v", body, readErr)
	}
	id := ""
	k.cacheMu.Lock()
	for taskID := range k.tasks {
		id = taskID
	}
	k.cacheMu.Unlock()
	if status, _ := gateGet(t, srv, id, "?stream=true&starting_after=0"); status != http.StatusConflict {
		t.Fatalf("failed task replay status=%d", status)
	}
	k.cacheMu.Lock()
	bytes := k.eventBytes
	k.cacheMu.Unlock()
	if bytes != 0 || creates.Load() != 1 {
		t.Fatalf("failed log kept after last reader: events=%d generations=%d", bytes, creates.Load())
	}
}

// A present cursor must be an integer; empty or valueless is not "absent".
func TestBackgroundEmptyCursorRejected(t *testing.T) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 1, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		_, _ = io.WriteString(w, gateEvent("response.completed", ""))
	})
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	resp, id := openBackgroundTask(t, srv, `{"input":"A","background":true,"stream":true}`)
	resp.Body.Close()
	gateTask(t, k, id)
	for _, suffix := range []string{"?stream=true&starting_after=", "?stream=true&starting_after"} {
		if status, body := gateGet(t, srv, id, suffix); status != http.StatusBadRequest {
			t.Errorf("cursor %q: status=%d body=%q", suffix, status, body)
		}
	}
}

// With a slow validator, one task's storm is bounded per task before it can
// take shared slots, so another task still reattaches.
func TestBackgroundSlowValidatorStorm(t *testing.T) {
	var creates, stalled atomic.Int32
	holdGeneration, holdBilling := make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(holdBilling); close(holdGeneration) })
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sub2api/billing" {
			if r.Header.Get("X-Storm") != "" {
				stalled.Add(1)
				select {
				case <-holdBilling:
				case <-r.Context().Done():
					return
				}
			}
			_, _ = io.WriteString(w, `{}`)
			return
		}
		creates.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		w.(http.Flusher).Flush()
		select {
		case <-holdGeneration:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	a, idA := openBackgroundTask(t, srv, `{"input":"A","background":true,"stream":true}`)
	a.Body.Close()
	b, idB := openBackgroundTask(t, srv, `{"input":"B","background":true,"stream":true}`)
	b.Body.Close()
	// Creators count against the per-task cap until they detach.
	for _, id := range []string{idA, idB} {
		for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(time.Millisecond) {
			k.cacheMu.Lock()
			task := k.tasks[id]
			k.cacheMu.Unlock()
			task.mu.Lock()
			readers := task.readers
			task.mu.Unlock()
			if readers == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("creator still attached to %s", id)
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for range maxSockets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/responses/"+idA+"?stream=true&starting_after=0", nil)
			req.Header.Set("Authorization", "Bearer owner")
			req.Header.Set("X-Storm", "A")
			if resp, err := http.DefaultClient.Do(req); err == nil {
				resp.Body.Close()
			}
		}()
	}
	defer wg.Wait()
	deadline := time.Now().Add(3 * time.Second)
	for stalled.Load() < maxTaskReaders {
		if time.Now().After(deadline) {
			t.Fatalf("storm never reached validator: %d", stalled.Load())
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(100 * time.Millisecond)
	if n := stalled.Load(); n > maxTaskReaders {
		t.Errorf("storm on one task reached validator %d times (cap %d)", n, maxTaskReaders)
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/responses/"+idB+"?stream=true&starting_after=0", nil)
	req.Header.Set("Authorization", "Bearer owner")
	res, err := http.DefaultClient.Do(req) // headers only; B's stream stays open
	release.Do(func() { close(holdBilling); close(holdGeneration) })
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusOK || creates.Load() != 2 {
		t.Fatalf("storm on A blocked B: status=%d generations=%d", res.StatusCode, creates.Load())
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func billingReply(status int) *http.Response {
	return &http.Response{StatusCode: status, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("{}"))}
}

// reviewTask builds a completed two-event task owned by "Bearer owner" whose
// billing validator is the given transport (no network).
func reviewTask(rt roundTripFunc) (*keeper, *responseTask) {
	u, _ := url.Parse("http://billing.invalid")
	owner := httptest.NewRequest(http.MethodGet, "/v1/responses/resp_ck_review", nil)
	owner.Header.Set("Authorization", "Bearer owner")
	k := &keeper{cfg: config{upstream: u}, work: newWorkingSet(), tasks: map[string]*responseTask{}, client: &http.Client{Transport: rt}}
	t := &responseTask{keeper: k, id: "resp_ck_review", owner: k.ownerOf(owner), cancel: func() {}, stream: true, done: true, count: 2, changed: make(chan struct{})}
	k.tasks[t.id] = t
	return k, t
}

func reviewCall(k *keeper, rawQuery, credential string) (status int) {
	r := httptest.NewRequest(http.MethodGet, "/v1/responses/resp_ck_review", nil)
	r.URL.RawQuery = rawQuery
	if credential != "" {
		r.Header.Set("Authorization", "Bearer "+credential)
	}
	w := httptest.NewRecorder()
	// The log-less fixture aborts a started stream; the status is already sent.
	defer func() {
		if p := recover(); p != nil && p != http.ErrAbortHandler {
			panic(p)
		}
		status = w.Code
	}()
	k.taskOperation(w, r)
	return w.Code
}

// Cancel is applied even when the task's local reader budget is full.
func TestBackgroundCancelAtReaderCap(t *testing.T) {
	k, task := reviewTask(func(*http.Request) (*http.Response, error) { return billingReply(http.StatusOK), nil })
	task.done, task.readers = false, maxTaskReaders
	r := httptest.NewRequest(http.MethodPost, "/v1/responses/resp_ck_review/cancel", nil)
	r.Header.Set("Authorization", "Bearer owner")
	w := httptest.NewRecorder()
	k.taskOperation(w, r)
	if w.Code != http.StatusOK || !task.cancelled {
		t.Fatalf("cancel at reader cap: status=%d cancelled=%v body=%s", w.Code, task.cancelled, w.Body)
	}
}

// Malformed query fields are dropped by URL.Query(); a hidden cursor must not
// turn into a full replay.
func TestBackgroundMalformedCursorQueryRejected(t *testing.T) {
	ok := roundTripFunc(func(*http.Request) (*http.Response, error) { return billingReply(200), nil })
	for _, q := range []string{"stream=true&starting_after=%ZZ", "stream=true&starting_after=%", "stream=true&starting_after=0;bad"} {
		k, task := reviewTask(ok)
		if got := reviewCall(k, q, "owner"); got != http.StatusBadRequest || task.pending != 0 || task.readers != 0 {
			t.Errorf("%s: status=%d pending=%d readers=%d", q, got, task.pending, task.readers)
		}
	}
}

// Single-upstream mode has no per-credential isolation: callers stalled at
// billing share the task's reader budget, and a stalled pool of them can
// starve later readers until they release or the upstream decides.
func TestBackgroundCallersShareTaskBudget(t *testing.T) {
	entered, release := make(chan struct{}, maxTaskReaders), make(chan struct{})
	k, _ := reviewTask(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer owner" {
			entered <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return nil, r.Context().Err()
			}
			return billingReply(http.StatusUnauthorized), nil
		}
		return billingReply(http.StatusOK), nil
	})
	var wg sync.WaitGroup
	defer func() { close(release); wg.Wait() }()
	for _, credential := range []string{"", "", "foreign", "foreign"} {
		wg.Add(1)
		go func() { defer wg.Done(); reviewCall(k, "", credential) }()
	}
	for range maxTaskReaders {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("foreign callers never reached billing")
		}
	}
	// The budget is shared: stalled callers fill it and the next reader is
	// denied until they release.
	if got := reviewCall(k, "", "owner"); got != http.StatusServiceUnavailable {
		t.Fatalf("shared budget should deny a further reader: status=%d", got)
	}
}

// The decoded event map stays charged while acceptance (which may materialize
// history) runs, and every path returns the budget to zero.
func TestParsedEventChargedDuringAccept(t *testing.T) {
	k := &keeper{work: newWorkingSet()}
	raw := []byte(`{"type":"response.completed","response":{"id":"resp_up","status":"completed","output":[]},"opaque":"` + strings.Repeat("x", 512<<10) + `"}`)
	for _, acceptErr := range []error{io.ErrClosedPipe, nil} {
		var c collector
		var observed int64
		wire, release, parsed, err := k.responseFrameParsed(context.Background(), &c, raw, true, false, func() (bool, error) {
			k.work.mu.Lock()
			observed = k.work.output
			k.work.mu.Unlock()
			return false, acceptErr
		})
		if err != acceptErr {
			t.Fatalf("accept error lost: %v", err)
		}
		if observed < int64(len(raw)) {
			t.Errorf("decoded map uncharged during accept: output=%d raw=%d", observed, len(raw))
		}
		if acceptErr == nil && (parsed == nil || !bytes.Equal(wire, raw)) {
			t.Errorf("unchanged event lost its parsed map")
		}
		if release != nil {
			release()
		}
		c.release(k.work)
		if k.work.output != 0 {
			t.Fatalf("output not balanced (acceptErr=%v): %d", acceptErr, k.work.output)
		}
	}
}

// Many medium values accumulate allocator slack beyond their encoded length;
// the held charge must cover the decoded map's real capacities.
func TestParsedChargeCoversManyMediumFields(t *testing.T) {
	var b strings.Builder
	b.WriteString(`{"type":"ping"`)
	for i := range 128 {
		b.WriteString(`,"opaque_` + strconv.Itoa(i) + `":"` + strings.Repeat("x", 8191) + `"`)
	}
	b.WriteByte('}')
	raw := []byte(b.String())
	m := newWorkingSet()
	var c collector
	obj, held, err := c.collectParsed(raw, m, true)
	if err != nil {
		t.Fatal(err)
	}
	var live int64
	for key, value := range obj {
		live += int64(len(key) + cap(value))
	}
	if held < live || m.output < held+c.charged {
		t.Errorf("held=%d live=%d output=%d collector=%d", held, live, m.output, c.charged)
	}
	m.freeOutput(held)
	c.release(m)
	if m.output != 0 {
		t.Fatalf("output not balanced: %d", m.output)
	}
}

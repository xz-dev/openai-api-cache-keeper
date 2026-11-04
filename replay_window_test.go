package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type replayWindowWriter struct {
	header                 http.Header
	entered                chan int
	release                chan struct{}
	writeOnce, releaseOnce sync.Once
	cancelled              atomic.Bool
}

func (w *replayWindowWriter) Header() http.Header { return w.header }
func (w *replayWindowWriter) WriteHeader(int)     {}
func (w *replayWindowWriter) Flush()              {}
func (w *replayWindowWriter) Write(p []byte) (int, error) {
	w.writeOnce.Do(func() { w.entered <- len(p) })
	<-w.release
	if w.cancelled.Load() {
		return 0, context.Canceled
	}
	return len(p), nil
}
func (w *replayWindowWriter) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() && !deadline.After(time.Now()) {
		w.cancelled.Store(true)
		w.releaseOnce.Do(func() { close(w.release) })
	}
	return nil
}

type replayWindowSink struct {
	header http.Header
	data   bytes.Buffer
}

func (w *replayWindowSink) Header() http.Header              { return w.header }
func (w *replayWindowSink) WriteHeader(int)                  {}
func (w *replayWindowSink) Flush()                           {}
func (w *replayWindowSink) Write(p []byte) (int, error)      { return w.data.Write(p) }
func (w *replayWindowSink) SetWriteDeadline(time.Time) error { return nil }

func TestReplayCopyWindowOwnedThroughBlockedCancellation(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	log, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	_, stop := context.WithCancel(context.Background())
	finished := make(chan struct{})
	close(finished)
	task := &responseTask{keeper: k, id: keeperResponsePrefix + "copy_window", cancel: stop, log: log, changed: make(chan struct{}), ready: make(chan struct{}), finished: finished, stream: true, done: true}
	t.Cleanup(task.expire)
	raw := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_up","object":"response","status":"completed","output":[{"text":%q}]}}`, strings.Repeat("x", 128<<10)))
	if err := task.record(context.Background(), raw, nil); err != nil {
		t.Fatal(err)
	}
	request := func(ctx context.Context) *http.Request {
		return httptest.NewRequest(http.MethodGet, "/v1/responses/"+task.id+"?stream=true", nil).WithContext(ctx)
	}
	task.owner = k.ownerOf(request(context.Background()))
	k.cacheMu.Lock()
	k.tasks[task.id] = task
	k.cacheMu.Unlock()
	baseline := admissionSnapshot(k)
	run := func(w http.ResponseWriter, ctx context.Context) (err error) {
		defer func() {
			if p := recover(); p != nil {
				if p == http.ErrAbortHandler {
					err = http.ErrAbortHandler
				} else {
					panic(p)
				}
			}
		}()
		k.taskOperation(w, request(ctx))
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	blocked := &replayWindowWriter{header: make(http.Header), entered: make(chan int, 1), release: make(chan struct{})}
	done := make(chan error, 1)
	var worker sync.WaitGroup
	t.Cleanup(func() { cancel(); blocked.releaseOnce.Do(func() { close(blocked.release) }); worker.Wait() })
	worker.Add(1)
	go func() { defer worker.Done(); done <- run(blocked, ctx) }()
	var window int
	select {
	case window = <-blocked.entered:
	case <-time.After(time.Second):
		t.Fatal("real authenticated replay never reached blocked writer")
	}
	if window != 32<<10 {
		t.Fatalf("copy path escaped explicit window: %d", window)
	}
	if got := admissionSnapshot(k).output - baseline.output; got != int64(window) {
		t.Fatalf("blocked copy window uncharged: bytes=%d outputDelta=%d", window, got)
	}
	// A second fitting reader must finish while the first destination is blocked.
	fitting := &replayWindowSink{header: make(http.Header)}
	if err := run(fitting, context.Background()); err != nil || !strings.Contains(fitting.data.String(), "response.completed") {
		t.Fatalf("fitting replay excluded: %v", err)
	}
	if got := admissionSnapshot(k).output; got != baseline.output+int64(window) {
		t.Fatalf("fitting reader leaked copy credit: %d", got)
	}
	pressure := maxCollectedBytes - int64(window) - baseline.output
	if !k.work.reserveOutput(pressure) {
		t.Fatal("could not establish exact byte-pressure fixture")
	}
	var pressureOnce sync.Once
	freePressure := func() { pressureOnce.Do(func() { k.work.freeOutput(pressure) }) }
	t.Cleanup(freePressure)
	refused := &replayWindowSink{header: make(http.Header)}
	started := time.Now()
	err = run(refused, context.Background())
	if !errors.Is(err, http.ErrAbortHandler) || refused.data.Len() != 0 || time.Since(started) > time.Second {
		t.Fatalf("exhausted real window did not fail fast before copying: err=%v bytes=%d", err, refused.data.Len())
	}
	if got := admissionSnapshot(k).output; got != maxCollectedBytes {
		t.Fatalf("refused reader changed live output credits: %d", got)
	}
	freePressure()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel actuator/blocked copy did not join")
	}
	worker.Wait()
	if got := admissionSnapshot(k); got != baseline {
		t.Fatalf("cancelled replay ownership leak: got=%+v want=%+v", got, baseline)
	}
	task.expire()
	waitAdmissionCredits(t, k, admissionCredits{})
	t.Logf("blocked%d-byte send window output-charged; fitting reader completed; exhausted reader refused; cancel/join returned all credits", window)
}

func TestReplayIdleFollowerHoldsNoCopyWindow(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	log, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	_, stop := context.WithCancel(context.Background())
	task := &responseTask{keeper: k, id: keeperResponsePrefix + "idle_window", cancel: stop, log: log, changed: make(chan struct{}), ready: make(chan struct{}), stream: true}
	t.Cleanup(task.expire)
	raw := []byte(`{"type":"response.created","response":{"id":"resp_up","object":"response","status":"in_progress","output":[]}}`)
	if err := task.record(context.Background(), raw, nil); err != nil {
		t.Fatal(err)
	}
	baseline := admissionSnapshot(k)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// Cursor 0 already covers the sole recorded event, so the live follower
	// writes nothing and must wait idle for future data without wire credit.
	sink := &replayWindowSink{header: make(http.Header)}
	done := make(chan error, 1)
	go func() {
		done <- task.follow(sink, httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), 0, true)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if admissionSnapshot(k).output != baseline.output {
			t.Fatalf("idle follower held copy-window credit: outputDelta=%d", admissionSnapshot(k).output-baseline.output)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case err := <-done:
		t.Fatalf("idle follower returned early: %v", err)
	default:
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("idle follower cancel result: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle follower did not join cancellation")
	}
	if sink.data.Len() != 0 {
		t.Fatalf("idle follower unexpectedly wrote %d bytes", sink.data.Len())
	}
	if got := admissionSnapshot(k); got != baseline {
		t.Fatalf("idle follower cleanup leak: got=%+v want=%+v", got, baseline)
	}
	task.expire()
	t.Log("caught-up live follower held zero copy-window credit across idle wait; cancel restored baseline")
}

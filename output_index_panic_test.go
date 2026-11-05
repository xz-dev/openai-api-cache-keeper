package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const hugeOutputIndex = "2305843009213693951" // 2^61-1: grown*24 wraps to 0

// Re-review M2: an output_index whose slots cannot fit the collection bound
// is refused by name before any slot arithmetic; 2^61-1 used to wrap the
// charge to 0 and panic in make.
func TestOutputIndexRefusedBeforeMultiplying(t *testing.T) {
	for _, index := range []string{hugeOutputIndex, fmt.Sprint(maxOutputIndex + 1)} {
		c := &collector{reserve: func(int64) bool { return true }}
		err := c.event([]byte(`{"type":"response.output_item.done","output_index":` + index + `,"item":{"type":"message"}}`))
		var named *outputIndexError
		if !errors.As(err, &named) {
			t.Fatalf("output_index %s: %v", index, err)
		}
		e := responseError(err)
		if e.status != 502 || e.code != "upstream_output_index_too_large" || e.hop != keeperHop || e.limit != maxCollectedBytes {
			t.Fatalf("output_index %s: %+v", index, e)
		}
	}
	c := &collector{reserve: func(int64) bool { return true }}
	if err := c.event([]byte(`{"type":"response.output_item.done","output_index":` + fmt.Sprint(maxOutputIndex) + `,"item":{}}`)); err != nil {
		t.Fatalf("the largest admitted index: %v", err)
	}
}

// lockedBuffer collects log output written from other goroutines.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// hugeIndexUpstream streams one output item at a wrapping index.
func hugeIndexUpstream(t *testing.T) func(*config) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 10, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		_, _ = io.WriteString(w, `data: {"type":"response.output_item.done","output_index":`+hugeOutputIndex+`,"item":{"type":"message"}}`+"\n\n")
		_, _ = io.WriteString(w, gateEvent("response.completed", ""))
	})
	return func(c *config) { c.upstream = u }
}

// waitWorkIdle waits until no spool or output byte is charged to the working set.
func waitWorkIdle(t *testing.T, k *keeper, what string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		k.work.mu.Lock()
		disk, output := k.work.disk, k.work.output
		k.work.mu.Unlock()
		if disk == 0 && output == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d spool bytes and %d output bytes still charged after 1 min", what, disk, output)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// The wrapping index on a foreground stream ends in the named failure event,
// and on a background task fails the task; neither crashes keeper.
func TestOutputIndexOverflowNamedOnTurns(t *testing.T) {
	_, srv, k := newTestKeeper(t, hugeIndexUpstream(t))
	status, out := postPath(t, srv, http.MethodPost, "/v1/responses", "Bearer owner", `{"model":"m","input":"A","stream":true}`, false)
	if status != 200 || !strings.Contains(out, `"code":"upstream_output_index_too_large"`) || !strings.Contains(out, `"hop":"keeper"`) {
		t.Fatalf("foreground: %d %.400s", status, out)
	}
	resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
	_, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	task := gateTask(t, k, id)
	task.mu.Lock()
	err := task.err
	task.mu.Unlock()
	if err == nil {
		t.Fatal("background task with a wrapping output_index did not fail")
	}
	waitWorkIdle(t, k, "background task after a wrapping output_index")
}

// Re-review M2: a panic in a background task is a keeper bug. It is logged
// with its stack, fails the task, and releases the task's resources; it no
// longer kills the process.
func TestBackgroundPanicRecoveredAndReleased(t *testing.T) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 10, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		_, _ = io.WriteString(w, gateEvent("response.completed", ""))
	})
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	logs := &lockedBuffer{}
	prev := log.Writer()
	log.SetOutput(logs)
	orig := collectEventCalls
	collectEventCalls = func() { panic("test: injected collector panic") }
	t.Cleanup(func() { collectEventCalls = orig; log.SetOutput(prev) })
	// The stream fails at its first event, so the creator's connection is
	// aborted like any failed background stream; the task records why.
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"model":"m","input":"A","background":true,"stream":true}`))
	req.Header.Set("Authorization", "Bearer owner")
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_, _ = io.ReadAll(resp.Body)
		resp.Body.Close()
	}
	k.cacheMu.Lock()
	var id string
	for tid := range k.tasks {
		id = tid
	}
	k.cacheMu.Unlock()
	task := gateTask(t, k, id)
	task.mu.Lock()
	err := task.err
	task.mu.Unlock()
	if !errors.Is(err, errBackgroundPanic) {
		t.Fatalf("task err %v, want errBackgroundPanic", err)
	}
	waitWorkIdle(t, k, "background task after a panic")
	if s := logs.String(); !strings.Contains(s, "BUG: background task "+id+" panicked: test: injected collector panic") || !strings.Contains(s, "goroutine ") {
		t.Fatalf("panic not logged with its stack:\n%.2000s", s)
	}
}

// Re-review M2: a panic in the WS handler still closes both sockets and
// releases the in-flight turn spool (cleanup is deferred).
func TestWSNativePanicReleasesTurn(t *testing.T) {
	_, srv, k := newWSNativeKeeper(t)
	orig := collectEventCalls
	collectEventCalls = func() { panic("test: injected collector panic") }
	t.Cleanup(func() { collectEventCalls = orig })
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"A"}`)); err != nil {
		t.Fatal(err)
	}
	for {
		if _, _, err := c.Read(ctx); err != nil {
			if ctx.Err() != nil {
				t.Fatal("socket not closed after the handler panicked")
			}
			break
		}
	}
	waitWorkIdle(t, k, "WS turn after a handler panic")
	deadline := time.Now().Add(time.Minute)
	for k.work.activeSockets.Load() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d socket slots still held", k.work.activeSockets.Load())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

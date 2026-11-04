package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Actual failed background completion must not hold cacheMu while another
// store writer is paused. New accounting path needs this invariant too.
func TestIndependentFailedBackgroundDeadlocksReservedWriter(t *testing.T) {
	if os.Getenv("KEEPER_LOCK_REGRESSION_CHILD") != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestIndependentFailedBackgroundDeadlocksReservedWriter$", "-test.v")
		cmd.Env = append(os.Environ(), "KEEPER_LOCK_REGRESSION_CHILD=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("bounded background/writer lock regression: %v\n%s", err, out)
		}
		return
	}
	upstreamRelease := make(chan struct{})
	var upOnce sync.Once
	defer upOnce.Do(func() { close(upstreamRelease) })
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprint(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_upstream\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-upstreamRelease:
		case <-r.Context().Done():
			return
		}
		fmt.Fprint(w, "data: {\"type\":\"error\",\"code\":\"upstream_failure\",\"message\":\"failed\"}\n\n")
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	resp, id := openBackgroundTask(t, srv, `{"input":"A","background":true,"stream":true,"store":false}`)
	defer resp.Body.Close()
	k.cacheMu.Lock()
	task := k.tasks[id]
	k.cacheMu.Unlock()
	entered, release := make(chan struct{}), make(chan struct{})
	var writerOnce, releaseOnce sync.Once
	finished := make(chan error, 1)
	defer func() { releaseOnce.Do(func() { close(release) }); <-finished }()
	k.store.mu.Lock()
	k.store.compactAt = 1
	k.store.compactCopy = func(dst, src *bolt.DB, limit int64) error {
		writerOnce.Do(func() { close(entered); <-release })
		return bolt.Compact(dst, src, limit)
	}
	k.store.mu.Unlock()
	go func() {
		source := sliceItems(items(`"slow"`))
		_, _, err := k.store.acceptWithBudget(context.Background(), "slow", [32]byte{}, source, "", true, false, func() (int64, func()) { return k.reserveHistoryCache(32) })
		finished <- err
	}()
	<-entered
	// The actual reserved writer is paused. Background cancellation must reach
	// synchronous cleanup without holding shared cache accounting while waiting.
	task.cancel()
	upOnce.Do(func() { close(upstreamRelease) })
	_ = task // Task existence established before writer pause; no task lock may wait in probe.
	deadline := time.Now().Add(time.Second)
	// Wait until failed producer is parked in real store cleanup, not the SSE path.
	var stack string
	for time.Now().Before(deadline) {
		b := make([]byte, 1<<20)
		n := runtime.Stack(b, true)
		stack = string(b[:n])
		if strings.Contains(stack, "(*store).forget") && strings.Contains(stack, "(*keeper).startBackground.func1") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !strings.Contains(stack, "(*store).forget") {
		t.Fatal("cleanup did not reach forget")
	}
	otherLog, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	other := &responseTask{keeper: k, log: otherLog}
	defer other.discard()
	appended := make(chan error, 1)
	go func() { other.mu.Lock(); err := other.append([]byte("fits")); other.mu.Unlock(); appended <- err }()
	blocked := false
	select {
	case err := <-appended:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		blocked = true
	}
	if blocked {
		t.Log("counterexample: failed background completion holds cacheMu while store.forget waits paused writer; unrelated fitting append cannot finish")
		for _, block := range strings.Split(stack, "\n\n") {
			if strings.Contains(block, "(*store).forget") {
				t.Log(block)
			}
		}
		releaseOnce.Do(func() { close(release) })
		if err := <-appended; err != nil {
			t.Fatal(err)
		}
		t.Error("cacheMu held across store mutation wait")
	}
}

package main

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The heap release is single-flight and starts at most once per interval;
// the request path never waits for it.
func TestHeapReleaseSingleFlightRateLimited(t *testing.T) {
	var now atomic.Int64
	now.Store(int64(time.Hour))
	var runs atomic.Int32
	gate := make(chan struct{})
	finished := make(chan struct{}, 8)
	h := &heapRelease{clock: now.Load, run: func() {
		runs.Add(1)
		<-gate
		finished <- struct{}{}
	}}
	h.request()
	// Running: concurrent requests start nothing and do not block.
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); h.request() }()
	}
	wg.Wait()
	close(gate)
	<-finished
	for h.busy.Load() {
		time.Sleep(time.Millisecond)
	}
	if n := runs.Load(); n != 1 {
		t.Fatalf("%d releases while one ran", n)
	}
	// Finished but inside the interval: still nothing.
	now.Add(int64(heapReleaseInterval) - 1)
	h.request()
	if h.busy.Load() || runs.Load() != 1 {
		t.Fatalf("release started inside the interval (runs %d)", runs.Load())
	}
	// One interval after the last start: the next request releases again.
	now.Add(1)
	h.request()
	<-finished
	if n := runs.Load(); n != 2 {
		t.Fatalf("runs %d after the interval, want 2", n)
	}
}

// A release that is still running refuses a second caller even once the
// interval has passed: the busy flag, not the clock, refuses it.
func TestHeapReleaseRefusedWhileRunning(t *testing.T) {
	var now atomic.Int64
	now.Store(int64(time.Hour))
	var runs atomic.Int32
	started := make(chan struct{}, 2)
	gate := make(chan struct{})
	h := &heapRelease{clock: now.Load, run: func() {
		runs.Add(1)
		started <- struct{}{}
		<-gate
	}}
	h.request()
	<-started
	now.Add(10 * int64(heapReleaseInterval))
	h.request()
	if n := runs.Load(); n != 1 || !h.busy.Load() {
		t.Fatalf("second caller during a release: runs %d busy %t", n, h.busy.Load())
	}
	select {
	case <-started:
		t.Fatal("second release started while the first ran")
	case <-time.After(20 * time.Millisecond):
	}
	close(gate)
	for h.busy.Load() {
		time.Sleep(time.Millisecond)
	}
	// The refused caller did not move the interval: the next request runs.
	h.request()
	<-started
	for h.busy.Load() {
		time.Sleep(time.Millisecond)
	}
	if n := runs.Load(); n != 2 {
		t.Fatalf("runs %d after the first finished, want 2", n)
	}
}

// A ticketed request asks for the release when its last holder lets go,
// not before.
func TestTicketEndRequestsHeapRelease(t *testing.T) {
	g := testGate(t, t.TempDir(), 1<<40, 4<<30)
	released := make(chan struct{}, 2)
	g.heap.run = func() { released <- struct{}{} }
	tk, e := g.admit(largeBodyBytes)
	if e != nil {
		t.Fatal(e.message)
	}
	if !tk.ref() {
		t.Fatal("ref refused")
	}
	tk.unref()
	select {
	case <-released:
		t.Fatal("released while a holder remains")
	case <-time.After(50 * time.Millisecond):
	}
	tk.unref()
	select {
	case <-released:
	case <-time.After(5 * time.Second):
		t.Fatal("no heap release after the last holder")
	}
	g.waiters.Wait()
}

package main

import (
	"runtime"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// heapRelease returns a large request's heap to the OS when the request ends
// (its ticket's last holder lets go). The Go GC runs only when allocation
// reaches the heap goal, and an idle keeper allocates nothing: measured after
// one 256 MiB turn, about 20 MiB of anon stayed for good. It was not live:
// garbage from the final link commit (bbolt node copies, the item-span plan,
// storeTx.ref/linkPlan rows) and bbolt's sync.Pool of 4 KiB page buffers
// that the commit filled. A pool keeps its contents through one GC (victim
// cache), so the release runs two: runtime.GC moves the pool into the victim,
// debug.FreeOSMemory drops the victim and returns every free span.
//
// The request path only reads the clock and does atomic loads and one CAS:
// the release is single-flight and starts at most once per
// heapReleaseInterval, in its own goroutine. There is no timer: a request
// that ends inside the interval leaves its garbage to the next release or
// the next ordinary GC.
type heapRelease struct {
	busy  atomic.Bool
	last  atomic.Int64 // heapReleaseClock() at the last start; 0 = never
	clock func() int64
	run   func()
}

const heapReleaseInterval = time.Second

var heapReleaseEpoch = time.Now()

func heapReleaseClock() int64 { return int64(time.Since(heapReleaseEpoch)) + 1 }

func releaseHeap() {
	runtime.GC()
	debug.FreeOSMemory()
}

// request starts one release unless one is running or started within the
// interval.
func (h *heapRelease) request() {
	clock, run := h.clock, h.run
	if clock == nil {
		clock = heapReleaseClock
	}
	if run == nil {
		run = releaseHeap
	}
	now := clock()
	due := func() bool { last := h.last.Load(); return last == 0 || now-last >= int64(heapReleaseInterval) }
	if !due() || !h.busy.CompareAndSwap(false, true) {
		return
	}
	// Re-check as the owner: a release may have finished between the two.
	if !due() {
		h.busy.Store(false)
		return
	}
	h.last.Store(now)
	go func() {
		defer h.busy.Store(false)
		run()
	}()
}

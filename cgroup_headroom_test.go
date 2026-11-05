package main

import (
	"strings"
	"testing"
)

// With keeper's cgroup near memory.max, a request is refused with 503
// insufficient_memory naming keeper, not admitted toward an OOM kill.
func TestCgroupHeadroomRefuses503(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	var current int64 = 384<<20 - cgroupMargin - 64<<10 // 64 KiB left above the margin
	k.work.cgroup = func() (int64, int64, bool) { return 384 << 20, current, true }
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"hi"}`)
	if status != 503 || !strings.Contains(body, "insufficient_memory") || !strings.Contains(body, "keeper") {
		t.Fatalf("near limit: %d %s", status, body)
	}
	current = 64 << 20 // ample headroom: served
	if status, body, _ = post(t, srv, "Bearer a", `{"model":"m","input":"hi"}`); status != 200 {
		t.Fatalf("headroom: %d %s", status, body)
	}
	waitWorking(t, k, 0)
}

// The real cgroup reader parses memory.max / memory.current when present.
func TestCgroupMemReader(t *testing.T) {
	c := openCgroupMem()
	if c == nil {
		t.Skip("no cgroup v2")
	}
	limit, cur, ok := c.read()
	if ok && (limit <= 0 || cur <= 0) {
		t.Fatalf("limit=%d current=%d", limit, cur)
	}
}

// Headroom counts the working set: inactive (clean, reclaimable) file pages
// are subtracted; a missing or bogus stat leaves memory.current unchanged.
func TestWorkingSetBytes(t *testing.T) {
	stat := []byte("anon 100\nfile 900\nactive_file 200\ninactive_file 700\n")
	if got := workingSetBytes(1000, stat); got != 300 {
		t.Fatalf("got %d, want 300", got)
	}
	if got := workingSetBytes(1000, []byte("anon 1000\n")); got != 1000 {
		t.Fatalf("no inactive_file: got %d", got)
	}
	if got := workingSetBytes(500, []byte("inactive_file 700\n")); got != 500 {
		t.Fatalf("inactive above current: got %d", got)
	}
}

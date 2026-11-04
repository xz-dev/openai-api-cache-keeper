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

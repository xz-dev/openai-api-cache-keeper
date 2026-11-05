package main

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Per-hop memory headroom (design D4): every byte credit keeper takes is
// also checked against its own cgroup, so concurrent work is refused with
// 503 insufficient_memory instead of crossing memory.max - 32 MiB.
const cgroupMargin int64 = 32 << 20

// cgroupMem reads keeper's own cgroup v2 memory.max, memory.current and
// memory.stat through fds opened once at startup (stat may be nil).
type cgroupMem struct{ max, cur, stat *os.File }

// openCgroupMem returns nil when keeper has no cgroup v2 memory limit.
func openCgroupMem() *cgroupMem {
	b, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil
	}
	var rel string
	for _, line := range strings.Split(string(b), "\n") {
		if p, ok := strings.CutPrefix(line, "0::"); ok {
			rel = p
		}
	}
	if rel == "" {
		return nil
	}
	dir := filepath.Join("/sys/fs/cgroup", rel)
	mx, err := os.Open(filepath.Join(dir, "memory.max"))
	if err != nil {
		return nil
	}
	cur, err := os.Open(filepath.Join(dir, "memory.current"))
	if err != nil {
		mx.Close()
		return nil
	}
	stat, _ := os.Open(filepath.Join(dir, "memory.stat"))
	return &cgroupMem{max: mx, cur: cur, stat: stat}
}

func preadInt(f *os.File) (int64, bool) {
	var buf [32]byte
	n, _ := f.ReadAt(buf[:], 0)
	s := string(bytes.TrimSpace(buf[:n]))
	if s == "max" {
		return 0, false
	}
	v, err := strconv.ParseInt(s, 10, 64)
	return v, err == nil
}

// read returns the limit and current usage; ok is false when unlimited or
// unreadable (then only the byte pools bound keeper).
func (c *cgroupMem) read() (limit, current int64, ok bool) {
	if c == nil {
		return 0, 0, false
	}
	if limit, ok = preadInt(c.max); !ok {
		return 0, 0, false
	}
	if current, ok = preadInt(c.cur); !ok {
		return 0, 0, false
	}
	if c.stat != nil {
		var buf [8192]byte
		n, _ := c.stat.ReadAt(buf[:], 0)
		current = workingSetBytes(current, buf[:n])
	}
	return limit, current, true
}

// workingSetBytes is memory.current minus inactive_file. Leader decision
// (M1 6b): clean, reclaimable file cache does not count against headroom.
func workingSetBytes(current int64, stat []byte) int64 {
	for _, line := range bytes.Split(stat, []byte("\n")) {
		if v, ok := bytes.CutPrefix(line, []byte("inactive_file ")); ok {
			if n, err := strconv.ParseInt(string(v), 10, 64); err == nil && n > 0 && n <= current {
				return current - n
			}
		}
	}
	return current
}

type insufficientMemoryError struct{ need, free int64 }

func (e *insufficientMemoryError) Error() string {
	return fmt.Sprintf("keeper: insufficient memory: %d bytes are needed and %d are free in keeper's cgroup after outstanding charges and the %d-byte margin", e.need, e.free, cgroupMargin)
}

func insufficientMemory(e *insufficientMemoryError) *apiError {
	return &apiError{status: http.StatusServiceUnavailable, typ: "server_error", code: "insufficient_memory", message: e.Error()}
}

// headroomLocked checks n more bytes against the cgroup: memory.current
// already includes bytes in use, so counting every outstanding credit again
// errs only toward refusing. Caller holds m.mu; limit/current were read
// before taking it.
func (m *workingSet) headroomLocked(n, limit, current int64, ok bool) error {
	if !ok {
		return nil
	}
	free := limit - current - m.input - m.output - cgroupMargin
	if n > free {
		return &insufficientMemoryError{need: n, free: max(0, free)}
	}
	return nil
}

func (m *workingSet) cgroupRead() (int64, int64, bool) {
	if m.cgroup != nil {
		return m.cgroup()
	}
	return m.cg.read()
}

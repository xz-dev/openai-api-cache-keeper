package main

import (
	"io"
	"os"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/unix"
)

// The spool constants follow the 256 MiB cap (design D1, D5).
func TestSpoolBudgetsFollowCap(t *testing.T) {
	const capBytes = 268435456
	m := newWorkingSet()
	if m.historyMax != capBytes {
		t.Fatalf("historyMax=%d", m.historyMax)
	}
	// One maximum HTTP turn holds its raw spool plus its rebuilt upstream
	// spool (k_disk 2) inside the transient disk budget.
	if m.spoolMax < 2*capBytes {
		t.Fatalf("spoolMax=%d cannot hold one maximum turn", m.spoolMax)
	}
	t.Setenv("STORE_DIR", t.TempDir())
	t.Setenv("UPSTREAM_URL", "http://127.0.0.1:1")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.maxRequestBytes != capBytes || cfg.storePinBytes != capBytes {
		t.Fatalf("defaults request=%d pin=%d", cfg.maxRequestBytes, cfg.storePinBytes)
	}
	if spoolDiskFloor != 1<<30 {
		t.Fatalf("floor=%d", spoolDiskFloor)
	}
}

// A spool write that would cross the 1 GiB reserve gets an explicit 507
// naming the reserve, and nothing is buffered in memory instead (D3).
func TestSpoolDiskFloorRefuses507(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	k.work.statfs = func(fd int, st *unix.Statfs_t) error {
		if err := unix.Fstatfs(fd, st); err != nil {
			return err
		}
		st.Frsize, st.Bavail = 1, uint64(spoolDiskFloor)+64
		return nil
	}
	status, body, _ := post(t, srv, "Bearer floor", `{"model":"m","input":"`+strings.Repeat("x", 4096)+`"}`)
	if status != 507 || !strings.Contains(body, "insufficient_storage") || !strings.Contains(body, "1073741824-byte disk reserve") {
		t.Fatalf("floor: %d %s", status, body)
	}
	waitWorking(t, k, 0)
}

func residentBytes(t *testing.T, f *os.File) int64 {
	t.Helper()
	st, err := f.Stat()
	if err != nil || st.Size() == 0 {
		return 0
	}
	m, err := unix.Mmap(int(f.Fd()), 0, int(st.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Munmap(m)
	page := os.Getpagesize()
	vec := make([]byte, (len(m)+page-1)/page)
	if _, _, e := unix.Syscall(unix.SYS_MINCORE, uintptr(unsafe.Pointer(&m[0])), uintptr(len(m)), uintptr(unsafe.Pointer(&vec[0]))); e != 0 {
		t.Fatal(e)
	}
	var n int64
	for _, v := range vec {
		n += int64(v & 1)
	}
	return n * int64(page)
}

// pageCacheDir returns a temp dir whose filesystem keeps written data in the
// page cache (ZFS uses its ARC instead, where mincore sees nothing).
func pageCacheDir(t *testing.T) string {
	for _, base := range []string{os.TempDir(), "/tmp", "/var/tmp"} {
		dir, err := os.MkdirTemp(base, "spool-cache-")
		if err != nil {
			continue
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		probe, err := os.Create(dir + "/probe")
		if err != nil {
			continue
		}
		_, _ = probe.Write(make([]byte, 1<<20))
		cached := residentBytes(t, probe) > 0
		probe.Close()
		os.Remove(probe.Name())
		if cached {
			return dir
		}
	}
	t.Skip("no page-cache-backed filesystem to observe")
	return ""
}

// Writers sync and evict each window and readers evict what they read, so
// a large spool keeps about one window cached (D3 page-cache bound).
func TestSpoolPageCacheBounded(t *testing.T) {
	dir := pageCacheDir(t)
	k := newKeeper(config{ttl: 1, maxStoreBytes: 1 << 20, maxRequestBytes: 64 << 20, storeDir: dir,
		storeMetaBytes: 1 << 20, storePinBytes: 1 << 20, storeCompactBytes: 4 << 20})
	t.Cleanup(func() { _ = k.store.close() })
	f, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	chunk := make([]byte, 32<<10)
	// Pipelined writeback keeps the window under writeback plus the one
	// being filled: about two windows, sampled at every MiB while writing.
	limit := 2 * spoolCacheWindow
	var peak int64
	for i := 0; i < (48<<20)/len(chunk); i++ {
		if _, err := f.Write(chunk); err != nil {
			t.Fatal(err)
		}
		if f.size%(1<<20) == 0 {
			peak = max(peak, residentBytes(t, f.File))
		}
	}
	t.Logf("write peak resident %d bytes (limit %d)", peak, limit)
	if peak > limit || peak == 0 {
		t.Fatalf("while writing: %d bytes cached, limit %d", peak, limit)
	}
	if _, err := io.Copy(io.Discard, f.section(0, f.size)); err != nil {
		t.Fatal(err)
	}
	if r := residentBytes(t, f.File); r > limit {
		t.Fatalf("after section read: %d bytes cached", r)
	}
	body, err := f.requestBody()
	if err != nil {
		t.Fatal(err)
	}
	if n, err := io.Copy(io.Discard, struct{ io.Reader }{body}); err != nil || n != f.size {
		t.Fatal(n, err)
	}
	if r := residentBytes(t, f.File); r > limit {
		t.Fatalf("after body read: %d bytes cached", r)
	}
	body.Close()
	// Many small items (text histories): per-span drops cover no whole page,
	// so only the spool-wide read window bounds these passes.
	buf := make([]byte, 1000)
	for off := int64(0); off+1000 <= f.size; off += 1000 {
		if _, err := io.Copy(io.Discard, f.section(off, 1000)); err != nil {
			t.Fatal(err)
		}
	}
	if r := residentBytes(t, f.File); r > limit {
		t.Fatalf("after small-span sections: %d bytes cached", r)
	}
	for off := int64(0); off+1000 <= f.size; off += 1000 {
		if _, err := f.ReadAt(buf, off); err != nil {
			t.Fatal(err)
		}
	}
	if r := residentBytes(t, f.File); r > limit {
		t.Fatalf("after small ReadAt pass: %d bytes cached", r)
	}
}

func TestCheckSpoolDirRefusesTmpfs(t *testing.T) {
	if err := checkSpoolDir(t.TempDir()); err != nil && !strings.Contains(err.Error(), "tmpfs") {
		t.Fatal(err)
	}
	var st unix.Statfs_t
	if unix.Statfs("/dev/shm", &st) != nil || st.Type != unix.TMPFS_MAGIC {
		t.Skip("no tmpfs at /dev/shm")
	}
	if err := checkSpoolDir("/dev/shm"); err == nil || !strings.Contains(err.Error(), "tmpfs") {
		t.Fatalf("tmpfs accepted: %v", err)
	}
}

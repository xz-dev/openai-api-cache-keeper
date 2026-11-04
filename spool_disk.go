package main

import (
	"fmt"
	"io"
	"os"

	"golang.org/x/sys/unix"
)

// Spool storage rules (design D3).
const (
	// spoolDiskFloor is the 1 GiB reserve: a spool write that would leave
	// less real free space is refused with 507, never buffered in memory.
	spoolDiskFloor int64 = 1 << 30
	// spoolCacheWindow bounds the page cache one spool keeps in the cgroup:
	// writers sync and drop each written window, readers drop each read one.
	spoolCacheWindow int64 = 8 << 20
)

// storageFloorError is the per-write disk floor refusal (507).
type storageFloorError struct{ free, need int64 }

func (e *storageFloorError) Error() string {
	return fmt.Sprintf("keeper: spool write of %d bytes would leave less than the %d-byte disk reserve (free %d bytes)", e.need, spoolDiskFloor, e.free)
}

// checkDiskFloor uses m.statfs when a test set it, else the real fstatfs.
func checkDiskFloor(m *workingSet, f *os.File, need int64) error {
	statfs := unix.Fstatfs
	if m.statfs != nil {
		statfs = m.statfs
	}
	var st unix.Statfs_t
	if err := statfs(int(f.Fd()), &st); err != nil {
		return err
	}
	free := int64(st.Bavail) * st.Frsize
	if free-need < spoolDiskFloor {
		return &storageFloorError{free: free, need: need}
	}
	return nil
}

// dropCache syncs and evicts [off, off+n) of f from the page cache.
func dropCache(f *os.File, off, n int64, sync bool) {
	if n <= 0 {
		return
	}
	fd := int(f.Fd())
	if sync {
		_ = unix.Fdatasync(fd)
	}
	_ = unix.Fadvise(fd, off, n, unix.FADV_DONTNEED)
}

// cacheDropReader reads [off, end) of f and evicts each read window, so a
// full pass over a large spool keeps at most spoolCacheWindow cached.
type cacheDropReader struct {
	f                 *os.File
	off, end, dropped int64
}

func newCacheDropReader(f *os.File, off, n int64) *cacheDropReader {
	return &cacheDropReader{f: f, off: off, end: off + n, dropped: off}
}

func (r *cacheDropReader) Read(p []byte) (int, error) {
	if r.off >= r.end {
		r.drop()
		return 0, io.EOF
	}
	if rem := r.end - r.off; int64(len(p)) > rem {
		p = p[:rem]
	}
	n, err := r.f.ReadAt(p, r.off)
	r.off += int64(n)
	if r.off-r.dropped >= spoolCacheWindow || r.off >= r.end || err != nil {
		r.drop()
	}
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
}

func (r *cacheDropReader) drop() {
	dropCache(r.f, r.dropped, r.off-r.dropped, false)
	r.dropped = r.off
}

// checkSpoolDir refuses a memory-backed spool directory at startup.
func checkSpoolDir(dir string) error {
	var st unix.Statfs_t
	if err := unix.Statfs(dir, &st); err != nil {
		return fmt.Errorf("spool dir %s: %w", dir, err)
	}
	if st.Type == unix.TMPFS_MAGIC || st.Type == unix.RAMFS_MAGIC {
		return fmt.Errorf("configuration error: STORE_DIR %s is on tmpfs/ramfs; spools must be on disk", dir)
	}
	return nil
}

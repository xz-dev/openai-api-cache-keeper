package main

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"

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

// writeBack is the pipelined writeback of one spool (design D3 page-cache
// bound): when window k = [next, end) is complete, its writeback starts
// (SYNC_FILE_RANGE_WRITE, no wait), then window k-1 = [pending, next) is
// waited for and evicted. At most about two windows stay cached.
//
// Spools are anonymous O_TMPFILE files that never outlive the process, so
// they need no durability point: sync_file_range only bounds the page
// cache, with no journal commit or device flush per window (Q3). A
// filesystem without sync_file_range falls back to fdatasync.
type writeBack struct {
	pending, next int64 // [pending, next) is under writeback; next = first unsubmitted byte
}

func (w *writeBack) advance(f *os.File, end int64) {
	if end-w.next < spoolCacheWindow {
		return
	}
	fd := int(f.Fd())
	if unix.SyncFileRange(fd, w.next, end-w.next, unix.SYNC_FILE_RANGE_WRITE) != nil {
		_ = unix.Fdatasync(fd)
	}
	if w.next > w.pending {
		if unix.SyncFileRange(fd, w.pending, w.next-w.pending, unix.SYNC_FILE_RANGE_WAIT_BEFORE|unix.SYNC_FILE_RANGE_WRITE|unix.SYNC_FILE_RANGE_WAIT_AFTER) != nil {
			_ = unix.Fdatasync(fd)
		}
		_ = unix.Fadvise(fd, w.pending, w.next-w.pending, unix.FADV_DONTNEED)
	}
	w.pending, w.next = w.next, end
}

// readWindow counts the bytes read from one spool through every read path.
// fadvise drops only whole pages inside its range, so a per-span drop of a
// small item frees nothing and a pass over many small items would keep the
// whole spool cached. Instead every spoolCacheWindow of reads drops the
// whole file's clean pages (design D3 read-side fadvise).
type readWindow struct{ n atomic.Int64 }

func (w *readWindow) note(f *os.File, n int) {
	if n > 0 && w.n.Add(int64(n)) >= spoolCacheWindow {
		w.n.Store(0)
		_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
	}
}

// cacheDropReader reads [off, end) of f, noting each read in its spool's
// window, so a full pass keeps at most about spoolCacheWindow cached.
type cacheDropReader struct {
	f        *os.File
	w        *readWindow
	off, end int64
}

func (r *cacheDropReader) Read(p []byte) (int, error) {
	if r.off >= r.end {
		return 0, io.EOF
	}
	if rem := r.end - r.off; int64(len(p)) > rem {
		p = p[:rem]
	}
	n, err := r.f.ReadAt(p, r.off)
	r.off += int64(n)
	r.w.note(r.f, n)
	if err == io.EOF && n > 0 {
		err = nil
	}
	return n, err
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

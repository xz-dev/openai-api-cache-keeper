package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"golang.org/x/sys/unix"
)

const spoolDirName = ".keeper-spools"

// Spools are anonymous O_TMPFILE files (design D3): they never have a name,
// so a crash frees them when the kernel closes the fd. A regular file in the
// spool directory therefore is not ours and refuses startup.
func spoolDir(dir string) (string, error) {
	ns := filepath.Join(dir, spoolDirName)
	if err := os.Mkdir(ns, 0700); err != nil && !os.IsExist(err) {
		return "", err
	}
	info, err := os.Lstat(ns)
	if err != nil {
		return "", err
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return "", fmt.Errorf("configuration error: spool dir %s must be a 0700 directory (mode %v)", ns, info.Mode())
	}
	return ns, nil
}

func openAnonymous(ns string) (*os.File, error) {
	fd, err := unix.Open(ns, unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0600)
	if err != nil {
		return nil, fmt.Errorf("configuration error: spool dir %s rejects O_TMPFILE: %w", ns, err)
	}
	return os.NewFile(uintptr(fd), filepath.Join(ns, "(anonymous)")), nil
}

// checkSpools is the startup assert: the spool directory holds no regular
// file, and its filesystem supports anonymous files. No sweep, no timer.
func checkSpools(dir string) error {
	ns, err := spoolDir(dir)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(ns)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Type().IsRegular() {
			return fmt.Errorf("configuration error: spool dir holds regular file %s; spools are anonymous, remove it", filepath.Join(ns, e.Name()))
		}
	}
	f, err := openAnonymous(ns)
	if err != nil {
		return err
	}
	return f.Close()
}

type spool struct {
	*os.File
	mem                     *workingSet
	size, offset, diskBytes int64
	dropped                 int64 // file offset below which written pages were synced and evicted
	reads                   readWindow
	skip                    int64 // reserved prefix bytes requestBody does not send
	mu                      sync.Mutex
	refs                    int
	ownerClosed             bool
	inputCredits            []func()
	state                   *stateLease
	frameState              *stateLease
	ticket                  *capTicket // admission ticket kept reserved while this spool lives
}

// attach makes this spool hold t until its file is closed and removed.
func (f *spool) attach(t *capTicket) {
	if t != nil && f.ticket == nil && t.ref() {
		f.ticket = t
	}
}

// setFirstMember fills the reserved frame prefix (see ticketPrefixLen) with
// `{"<key>":"<value>",`, so the member is the frame's FIRST member: the
// bridge reads it from a small prefix before spooling (contract with the
// bridge). value must have the reserved length.
func (f *spool) setFirstMember(key, value string) error {
	member := `{"` + key + `":"` + value + `",`
	if int64(len(member)) != f.skip+1 {
		return errors.New("frame has no reserved first-member prefix")
	}
	if _, err := f.File.WriteAt([]byte(member), f.offset); err != nil {
		return err
	}
	f.skip = 0
	return nil
}

func (k *keeper) newSpool() (_ *spool, err error) {
	state, err := k.work.state(spoolStateBytes(k.cfg.storeDir), nil)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			state.Close()
		}
	}()
	ns, err := spoolDir(k.cfg.storeDir)
	if err != nil {
		return nil, err
	}
	file, err := openAnonymous(ns)
	if err != nil {
		return nil, err
	}
	return &spool{File: file, state: state, mem: k.work, refs: 1}, nil
}
func (f *spool) Write(p []byte) (int, error) {
	f.mem.mu.Lock()
	if f.mem.disk+int64(len(p)) > maxSpoolBytes {
		f.mem.mu.Unlock()
		return 0, errors.New("transient disk budget exceeded")
	}
	f.mem.disk += int64(len(p))
	f.mem.diskPeak = max(f.mem.diskPeak, f.mem.disk)
	f.mem.mu.Unlock()
	n, err := 0, checkDiskFloor(f.mem, f.File, int64(len(p)))
	if err == nil {
		n, err = f.File.Write(p)
	}
	f.size += int64(n)
	f.diskBytes += int64(n)
	f.mem.mu.Lock()
	f.mem.disk -= int64(len(p) - n)
	f.mem.mu.Unlock()
	if end := f.offset + f.size; end-f.dropped >= spoolCacheWindow {
		dropCache(f.File, f.dropped, end-f.dropped, true)
		f.dropped = end
	}
	return n, err
}

// ReadAt evicts large reads at once (design D3 read-side fadvise).
func (f *spool) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off+f.offset)
	f.reads.note(f.File, n)
	return n, err
}

// section streams [off, off+n) of the payload with read-side eviction.
func (f *spool) section(off, n int64) io.Reader {
	return &cacheDropReader{f: f.File, w: &f.reads, off: f.offset + off, end: f.offset + off + n}
}
func (f *spool) Close() error {
	if f == nil {
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.ownerClosed {
		return nil
	}
	f.ownerClosed = true
	return f.releaseLocked()
}
func (f *spool) releaseLocked() error {
	f.refs--
	if f.refs > 0 {
		return nil
	}
	file := f.File
	f.File = nil
	closeErr := file.Close()
	for _, free := range f.inputCredits {
		free()
	}
	f.inputCredits = nil
	if f.frameState != nil {
		f.frameState.Close()
		f.frameState = nil
	}
	if f.state != nil {
		f.state.Close()
		f.state = nil
	}
	// The last close frees an anonymous file's blocks even when it reports an error.
	f.mem.mu.Lock()
	f.mem.disk -= f.diskBytes
	f.mem.mu.Unlock()
	if f.ticket != nil {
		f.ticket.unref()
		f.ticket = nil
	}
	return closeErr
}

// The transport owns a reference until Request.Body.Close, even if RoundTrip
// returns early. Handler completion alone cannot close/uncharge its open file.
type spoolBody struct {
	*io.SectionReader
	spool *spool
	file  *os.File
	once  sync.Once
	err   error
}

// Read evicts each sent window (design D3 read-side fadvise).
func (b *spoolBody) Read(p []byte) (int, error) {
	n, err := b.SectionReader.Read(p)
	b.spool.reads.note(b.file, n)
	return n, err
}

func (f *spool) requestBody() (*spoolBody, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.File == nil {
		return nil, os.ErrClosed
	}
	f.refs++
	return &spoolBody{SectionReader: io.NewSectionReader(f.File, f.offset+f.skip, f.size-f.skip), spool: f, file: f.File}, nil
}
func (b *spoolBody) Close() error {
	b.once.Do(func() { b.spool.mu.Lock(); defer b.spool.mu.Unlock(); b.err = b.spool.releaseLocked() })
	return b.err
}

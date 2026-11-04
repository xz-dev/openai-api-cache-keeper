package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/sys/unix"
)

const spoolDirName = ".keeper-spools"
const spoolMarker = "response-keeper-spool-v1:"

// A private marked namespace plus per-file marker identifies our stale files.
// Merely sharing a filename prefix, directory, or UID is not ownership proof.
func spoolNamespace(dir string) (string, []byte, error) {
	ns := filepath.Join(dir, spoolDirName)
	err := os.Mkdir(ns, 0700)
	if err == nil {
		var token [32]byte
		if _, err := rand.Read(token[:]); err != nil {
			return "", nil, err
		}
		marker := []byte(spoolMarker + hex.EncodeToString(token[:]) + "\n")
		f, err := os.OpenFile(filepath.Join(ns, "owner"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return "", nil, err
		}
		_, writeErr := f.Write(marker)
		closeErr := f.Close()
		if writeErr != nil {
			return "", nil, writeErr
		}
		if closeErr != nil {
			return "", nil, closeErr
		}
		return ns, marker, nil
	}
	if !os.IsExist(err) {
		return "", nil, err
	}
	info, err := os.Lstat(ns)
	if err != nil || !info.IsDir() {
		return "", nil, errors.New("spool namespace is not an owned directory")
	}
	f, err := openSpoolRegular(filepath.Join(ns, "owner"))
	if err != nil {
		return "", nil, err
	}
	defer f.Close()
	marker, err := io.ReadAll(io.LimitReader(f, int64(len(spoolMarker)+66)))
	if err != nil || len(marker) != len(spoolMarker)+65 || !bytes.HasPrefix(marker, []byte(spoolMarker)) || marker[len(marker)-1] != '\n' {
		return "", nil, errors.New("unrecognized spool namespace owner")
	}
	if _, err := hex.DecodeString(string(marker[len(spoolMarker) : len(marker)-1])); err != nil {
		return "", nil, err
	}
	return ns, marker, nil
}
func openSpoolRegular(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	f := os.NewFile(uintptr(fd), path)
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("not a regular spool file")
	}
	return f, nil
}
func cleanupTransient(dir string) error {
	ns, marker, err := spoolNamespace(dir)
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(ns)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "turn-") {
			continue
		}
		path := filepath.Join(ns, entry.Name())
		before, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !before.Mode().IsRegular() {
			continue
		}
		f, err := openSpoolRegular(path)
		if err != nil {
			return err
		}
		actual, statErr := f.Stat()
		header := make([]byte, len(marker))
		_, readErr := io.ReadFull(f, header)
		f.Close()
		if statErr != nil || readErr != nil || !os.SameFile(before, actual) || !bytes.Equal(header, marker) {
			continue
		}
		current, err := os.Lstat(path)
		if err != nil {
			return err
		}
		if !os.SameFile(actual, current) || !current.Mode().IsRegular() {
			continue
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

type spool struct {
	*os.File
	mem                     *workingSet
	size, offset, diskBytes int64
	dropped                 int64 // file offset below which written pages were synced and evicted
	mu                      sync.Mutex
	refs                    int
	ownerClosed             bool
	info                    os.FileInfo
	inputCredits            []func()
	state                   *stateLease
	frameState              *stateLease
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
	ns, marker, err := spoolNamespace(k.cfg.storeDir)
	if err != nil {
		return nil, err
	}
	file, err := os.CreateTemp(ns, "turn-*")
	if err != nil {
		return nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		os.Remove(file.Name())
		return nil, err
	}
	f := &spool{File: file, state: state, mem: k.work, refs: 1, info: info, offset: int64(len(marker))}
	if _, err := f.Write(marker); err != nil {
		f.Close()
		return nil, err
	}
	f.size = 0
	return f, nil
}
func (f *spool) Write(p []byte) (int, error) {
	f.mem.mu.Lock()
	if f.mem.disk+int64(len(p)) > maxSpoolBytes {
		f.mem.mu.Unlock()
		return 0, errors.New("transient disk budget exceeded")
	}
	f.mem.disk += int64(len(p))
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
	if int64(n) >= 1<<20 {
		dropCache(f.File, off+f.offset, int64(n), false)
	}
	return n, err
}

// section streams [off, off+n) of the payload with read-side eviction.
func (f *spool) section(off, n int64) io.Reader {
	return newCacheDropReader(f.File, f.offset+off, n)
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
	current, err := os.Lstat(file.Name())
	if err == nil && current.Mode().IsRegular() && os.SameFile(f.info, current) {
		err = os.Remove(file.Name())
	} else if err == nil {
		err = errors.New("spool path replaced; refusing unowned cleanup")
	}
	if err == nil {
		f.mem.mu.Lock()
		f.mem.disk -= f.diskBytes
		f.mem.mu.Unlock()
	} else {
		log.Printf("transient cleanup failed; bytes remain charged: %v", err)
	}
	if closeErr != nil {
		return closeErr
	}
	return err
}

// The transport owns a reference until Request.Body.Close, even if RoundTrip
// returns early. Handler completion alone cannot close/uncharge its open file.
type spoolBody struct {
	*io.SectionReader
	spool   *spool
	file    *os.File
	dropped int64 // payload offset below which read pages were evicted
	once    sync.Once
	err     error
}

// Read evicts each sent window (design D3 read-side fadvise).
func (b *spoolBody) Read(p []byte) (int, error) {
	n, err := b.SectionReader.Read(p)
	pos, _ := b.SectionReader.Seek(0, io.SeekCurrent)
	if pos-b.dropped >= spoolCacheWindow || (err != nil && pos > b.dropped) {
		dropCache(b.file, b.spool.offset+b.dropped, pos-b.dropped, false)
		b.dropped = pos
	}
	return n, err
}

func (f *spool) requestBody() (*spoolBody, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.File == nil {
		return nil, os.ErrClosed
	}
	f.refs++
	return &spoolBody{SectionReader: io.NewSectionReader(f.File, f.offset, f.size), spool: f, file: f.File}, nil
}
func (b *spoolBody) Close() error {
	b.once.Do(func() { b.spool.mu.Lock(); defer b.spool.mu.Unlock(); b.err = b.spool.releaseLocked() })
	return b.err
}
func spoolFilePattern(dir string) string { return filepath.Join(dir, spoolDirName, "turn-*") }

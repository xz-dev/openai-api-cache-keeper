package main

import (
	"bytes"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReviewConcurrentCorruptOpenKeepsExclusivePath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "keeper.db")
	if e := os.WriteFile(path, bytes.Repeat([]byte("invalid header"), 2048), 0600); e != nil {
		t.Fatal(e)
	}
	f, e := os.OpenFile(path, os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	defer f.Close()
	if e = unix.Flock(int(f.Fd()), unix.LOCK_EX); e != nil {
		t.Fatal(e)
	}
	type result struct {
		s *store
		e error
	}
	ch := make(chan result, 2)
	for i := 0; i < 2; i++ {
		go func() { s, e := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20); ch <- result{s, e} }()
	}
	// Both opens now wait for bbolt flock on same corrupt inode.
	time.Sleep(200 * time.Millisecond)
	if e = unix.Flock(int(f.Fd()), unix.LOCK_UN); e != nil {
		t.Fatal(e)
	}
	first := <-ch
	second := <-ch
	if first.s != nil {
		defer first.s.close()
	}
	if second.s != nil {
		defer second.s.close()
	}
	if first.e == nil && second.e == nil {
		t.Fatal("both live keepers opened same directory after corrupt-header recovery; recovery unlinked other keeper's newly opened file")
	}
	t.Logf("startup results: %v / %v", first.e, second.e)
}

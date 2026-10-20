package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReviewSecondOpenRespectsFileLock(t *testing.T) {
	dir := t.TempDir()
	s, e := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	if e != nil {
		t.Fatal(e)
	}
	defer s.close()
	o := [32]byte{1}
	mustReviewPut(t, s, "original", o, "", items(`"original-data"`))
	started := time.Now()
	s2, e := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	if e == nil {
		s2.close()
		t.Fatal("second open unexpectedly succeeded")
	}
	t.Logf("second open rejected after %s: %v", time.Since(started), e)
	if _, ok := s.get("original", o); !ok {
		t.Fatal("locked store changed")
	}
}

func TestReviewInvalidHeaderFailsSafely(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "keeper.db")
	original := bytes.Repeat([]byte("invalid header"), 2048)
	if e := os.WriteFile(p, original, 0600); e != nil {
		t.Fatal(e)
	}
	s, e := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	if e == nil {
		s.close()
		t.Fatal("corrupt startup must fail safely")
	}
	after, e := os.ReadFile(p)
	if e != nil || !bytes.Equal(after, original) {
		t.Fatal("failed open altered corrupt path")
	}
}

func TestReviewCopiesOutOfTransaction(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "a", o, "", items(`"A"`))
	got, ok := s.get("a", o)
	if !ok {
		t.Fatal("get a")
	}
	got[0][1] = 'B'
	again, ok := s.get("a", o)
	if !ok || string(again[0]) != `"A"` {
		t.Fatalf("read bytes alias disk: %s", again)
	}
}

func TestReviewSnapshotReadConcurrentEviction(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "a", o, "", items(`"A"`))
	got, ok := s.get("a", o)
	if !ok {
		t.Fatal("snapshot missing")
	}
	if err := dropTestResponse(s, "a"); err != nil {
		t.Fatal(err)
	}
	// get now resolves and copies in ONE protected transaction. Eviction after
	// that seam cannot invalidate its owned complete snapshot.
	checkItems(t, got, []string{`"A"`})
	if _, ok := s.get("a", o); ok {
		t.Fatal("evicted alias still readable")
	}
}

func TestReviewCompactionFailureKeepsOriginal(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "a", o, "", items(`"A"`))
	// Nonempty directory forces target open failure without filling host disk.
	tmp := s.path + ".compact"
	if e := os.Mkdir(tmp, 0700); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(filepath.Join(tmp, "keep-nonempty"), []byte("x"), 0600); e != nil {
		t.Fatal(e)
	}
	s.mu.Lock()
	err := s.compactLocked()
	s.mu.Unlock()
	if err == nil {
		t.Fatal("expected compact-target failure")
	}
	s.compactAt = 1
	if _, err := s.put("rejected", o, "", items(`"B"`)); err == nil {
		t.Fatal("required compaction failure did not reject admission")
	}
	if _, ok := s.get("rejected", o); ok {
		t.Fatal("failed admission registered id")
	}
	if _, ok := s.get("a", o); !ok {
		t.Fatal("compaction failure lost original")
	}
	if _, e := os.Stat(s.path); e != nil {
		t.Fatal(e)
	}
}

func TestReviewRegisteredPutFailureVisibleWS(t *testing.T) {
	_, srv, _ := newTestKeeper(t, func(c *config) { c.maxStoreBytes = 1 })
	c := dialWS(t, srv, "Bearer registered-refusal")
	ev := wsCreate(t, c, `{"type":"response.create","input":"A"}`)
	if ev["type"] != "error" || ev["error"].(map[string]any)["code"] != "history_storage_failed" {
		t.Fatalf("WS storage failure not visible: %v", ev)
	}
}

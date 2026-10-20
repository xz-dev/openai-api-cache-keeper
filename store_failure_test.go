package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	berrors "go.etcd.io/bbolt/errors"
	"golang.org/x/sys/unix"
)

func TestStoreCommitFailureRollsBackReplacementAndEviction(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprint("replacement=", replacement), func(t *testing.T) {
			s := newTestStore(t, time.Hour, 200<<10, 1<<20, 100<<10, 4<<20)
			o := [32]byte{1}
			original, _ := json.Marshal(strings.Repeat("A", 100<<10))
			mustReviewPut(t, s, "accepted", o, "", []json.RawMessage{original})
			// bbolt MaxSize rejects page allocation during Commit, AFTER our callback
			// has changed refs, counters and aliases and (for admission) evicted victim.
			s.db.MaxSize = int(s.diskUsage())
			replacementJSON, _ := json.Marshal(strings.Repeat("B", 150<<10))
			id := "incoming"
			if replacement {
				id = "accepted"
			}
			if _, err := s.put(id, o, "", []json.RawMessage{replacementJSON}); !errors.Is(err, berrors.ErrMaxSizeReached) {
				t.Fatalf("wanted real commit allocation rejection: %v", err)
			}
			roundtrip(t, s, "accepted", o, []string{string(original)})
			if !replacement {
				if _, ok := s.get(id, o); ok {
					t.Fatal("rejected id published")
				}
			}
			inspectStore(t, s)
			s.db.MaxSize = 0
			mustReviewPut(t, s, "after", o, "accepted", []json.RawMessage{original, json.RawMessage(`"C"`)})
			roundtrip(t, s, "after", o, []string{string(original), `"C"`})
			inspectStore(t, s)
		})
	}
}

func TestStoreDeleteAndPinReleaseRollbackAfterMutation(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "accepted", o, "", items(`"A"`))
	pin, release, err := s.putPinned(o, "", "", items(`"B"`))
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	// Real bbolt callback rollback, including changes already made by deletion.
	rejected := errors.New("reject transaction after delete")
	err = s.update(func(st *storeTx) error {
		if err := st.dropResponse([]byte("accepted")); err != nil {
			return err
		}
		if err := st.dropPin(pin); err != nil {
			return err
		}
		return rejected
	})
	if err != rejected {
		t.Fatal(err)
	}
	roundtrip(t, s, "accepted", o, []string{`"A"`})
	got, ok := s.getPinned(pin, o)
	if !ok {
		t.Fatal("pin lost on rollback")
	}
	checkItems(t, got, []string{`"B"`})
	inspectStore(t, s)
}

func TestStoreSocketPinNetBudgetAndFailedReplacement(t *testing.T) {
	s := newTestStore(t, time.Minute, 100, 1<<20, 8, 4<<20)
	o := [32]byte{1}
	_, pin, err := s.accept("private", o, sliceItems(items(`"A"`)), "", false, true)
	if err != nil {
		t.Fatal(err)
	}
	_, next, err := s.accept("private2", o, sliceItems(items(`"A"`, `"B"`)), pin, false, true)
	if err != nil {
		t.Fatalf("net replacement fits (6 bytes), not old+new (9): %v", err)
	}
	if _, ok := s.getPinned(pin, o); ok {
		t.Fatal("old pin not released on success")
	}
	for _, registered := range []bool{false, true} {
		if _, _, err := s.accept("failed", o, sliceItems(items(`"A"`, `"B"`, `"C"`)), next, registered, true); err != errStoreBudget {
			t.Fatalf("expected failed replacement: %v", err)
		}
		got, ok := s.getPinned(next, o)
		if !ok {
			t.Fatal("previous pin lost")
		}
		checkItems(t, got, []string{`"A"`, `"B"`})
		if _, ok := s.get("failed", o); ok {
			t.Fatal("pin failure published registered alias")
		}
		inspectStore(t, s)
	}
	if err := s.release(next); err != nil {
		t.Fatal(err)
	}
	inspectStore(t, s)
}

func TestStorePinMetadataBound(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1024, 1<<19, 4<<20)
	o := [32]byte{1}
	var releases []func()
	var accepted int
	defer func() {
		for _, release := range releases {
			release()
		}
	}()
	for i := 0; i < 100; i++ {
		pin, release, err := s.putPinned(o, "", "", items(`"A"`))
		if err != nil {
			if err != errStoreBudget {
				t.Fatal(err)
			}
			break
		}
		accepted++
		releases = append(releases, release)
		got, ok := s.getPinned(pin, o)
		if !ok {
			t.Fatal("admitted pin missing")
		}
		checkItems(t, got, []string{`"A"`})
		inspectStore(t, s)
	}
	if accepted == 0 || int64(accepted)*pinMetaOverhead > s.maxMeta || accepted == 100 {
		t.Fatalf("unbounded pins: %d", accepted)
	}
}

func TestStoreSharedLargePrefixPhysicalGrowth(t *testing.T) {
	s := newTestStore(t, time.Hour, 32<<20, 1<<20, 16<<20, 128<<20)
	o := [32]byte{1}
	prefix, _ := json.Marshal(strings.Repeat("X", 8<<20))
	var unique = int64(len(prefix))
	var hist = []json.RawMessage{prefix}
	for i := 0; i < 24; i++ {
		delta := json.RawMessage(fmt.Sprintf(`{"turn":%d}`, i))
		hist = append(hist, delta)
		unique += int64(len(delta))
		// No previous id: full-history fallback must still dedup growing prefixes.
		id := fmt.Sprint("large", i)
		mustReviewPut(t, s, id, o, "", hist)
		got, ok := s.get(id, o)
		if !ok || len(got) != len(hist) {
			t.Fatal("admitted full history missing")
		}
		for j := range got {
			if string(got[j]) != string(hist[j]) {
				t.Fatal("history content changed")
			}
		}
	}
	live, _, _, nodes, _ := inspectStore(t, s)
	if live != unique || nodes != 26 || s.diskUsage() > 4*unique {
		t.Fatalf("prefix duplication: live=%d unique=%d nodes=%d physical=%d", live, unique, nodes, s.diskUsage())
	}
}

func TestStoreCompactionRenameFailureKeepsOwnership(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "accepted", o, "", items(`"A"`))
	canonical := s.path
	// Existing nonempty directory makes rename fail AFTER Compact copied data.
	blocked := filepath.Join(s.dir, "blocked")
	if err := os.Mkdir(blocked, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(blocked, "keep"), []byte("x"), 0600); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	original := s.db
	s.path = blocked
	err := s.compactLocked()
	s.path = canonical
	s.mu.Unlock()
	if err == nil || s.db != original {
		t.Fatalf("failed swap changed active DB: %v", err)
	}
	roundtrip(t, s, "accepted", o, []string{`"A"`})
	if _, err := os.Stat(canonical); err != nil {
		t.Fatal(err)
	}
	if other, err := newStore(s.dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20); err == nil {
		other.close()
		t.Fatal("failed swap lost ownership")
	}
	inspectStore(t, s)
}

func TestStoreOwnershipThroughoutCompaction(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "stable", o, "", items(`"A"`))
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		for i := 0; i < 100; i++ {
			other, err := newStore(s.dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
			if err == nil {
				other.close()
				errCh <- errors.New("second owner entered during compaction")
				return
			}
		}
	}()
	for i := 0; i < 40; i++ {
		s.mu.Lock()
		err := s.compactLocked()
		s.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	<-done
	select {
	case err := <-errCh:
		t.Fatal(err)
	default:
	}
	roundtrip(t, s, "stable", o, []string{`"A"`})
}

// Child process confines RLIMIT_FSIZE to this test. This is a real OS write
// failure (EFBIG), NOT an ENOSPC claim and not a read-only simulation.
func TestStoreCompactionCopyWriteFailure(t *testing.T) {
	if os.Getenv("KEEPER_COMPACT_WRITE_CHILD") == "1" {
		s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
		o := [32]byte{1}
		raw, _ := json.Marshal(strings.Repeat("X", 256<<10))
		mustReviewPut(t, s, "accepted", o, "", []json.RawMessage{raw})
		var limit unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		limit.Cur = 64 << 10
		if err := unix.Setrlimit(unix.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		s.mu.Lock()
		original := s.db
		err := s.compactLocked()
		s.mu.Unlock()
		if !errors.Is(err, unix.EFBIG) {
			t.Fatalf("expected compact-copy OS EFBIG: %v", err)
		}
		if s.db != original {
			t.Fatal("failed copy replaced DB")
		}
		roundtrip(t, s, "accepted", o, []string{string(raw)})
		if _, err := os.Stat(s.path + ".compact"); !os.IsNotExist(err) {
			t.Fatalf("partial copy retained: %v", err)
		}
		if other, err := newStore(s.dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20); err == nil {
			other.close()
			t.Fatal("failed copy lost ownership")
		}
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreCompactionCopyWriteFailure$", "-test.v")
	cmd.Env = append(os.Environ(), "KEEPER_COMPACT_WRITE_CHILD=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("copy failure subprocess: %v\n%s", err, output)
	}
	t.Log(string(output))
}

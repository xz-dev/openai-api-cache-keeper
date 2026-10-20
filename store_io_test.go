package main

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestStoreCommitOSWriteFailure(t *testing.T) {
	if os.Getenv("KEEPER_COMMIT_WRITE_CHILD") == "1" {
		s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
		o := [32]byte{1}
		mustReviewPut(t, s, "accepted", o, "", items(`"A"`))
		_, pin, err := s.accept("private", o, sliceItems(items(`"B"`)), "", false, true)
		if err != nil {
			t.Fatal(err)
		}
		var limit unix.Rlimit
		if err := unix.Getrlimit(unix.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		limit.Cur = uint64(s.diskUsage())
		if err := unix.Setrlimit(unix.RLIMIT_FSIZE, &limit); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(strings.Repeat("X", 256<<10))
		// New alias and replacement pin are mutated in the callback, then bbolt's
		// file growth/write fails at commit. Old alias and pin must BOTH survive.
		if _, _, err := s.accept("accepted", o, sliceItems([]json.RawMessage{raw}), pin, true, true); !errors.Is(err, unix.EFBIG) {
			t.Fatalf("expected real commit OS EFBIG: %v", err)
		}
		roundtrip(t, s, "accepted", o, []string{`"A"`})
		got, ok := s.getPinned(pin, o)
		if !ok {
			t.Fatal("accepted pin lost on commit write error")
		}
		checkItems(t, got, []string{`"B"`})
		inspectStore(t, s)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreCommitOSWriteFailure$", "-test.v")
	cmd.Env = append(os.Environ(), "KEEPER_COMMIT_WRITE_CHILD=1")
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("commit failure subprocess: %v\n%s", err, output)
	}
	t.Log(string(output))
}

func TestStoreUniqueExpiryReclaimsIndexes(t *testing.T) {
	s := newTestStore(t, time.Minute, 1<<20, 2048, 1<<19, 32<<10)
	o := [32]byte{1}
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }
	for i := 0; i < 200; i++ {
		raw, _ := json.Marshal(strings.Repeat("X", 100) + string(rune('A'+i)))
		mustReviewPut(t, s, "current", o, "", []json.RawMessage{raw})
		roundtrip(t, s, "current", o, []string{string(raw)})
		_, _, _, nodes, aliases := inspectStore(t, s)
		if nodes != 2 || aliases != 1 {
			t.Fatalf("unique expiry leaked indexes: nodes=%d aliases=%d", nodes, aliases)
		}
		now = now.Add(2 * time.Minute)
	}
	if _, ok := s.get("current", o); ok {
		t.Fatal("expired history found")
	}
	live, meta, pinned, nodes, aliases := inspectStore(t, s)
	if live != 0 || meta != 0 || pinned != 0 || nodes != 0 || aliases != 0 {
		t.Fatalf("expired records retained: %d/%d/%d/%d/%d", live, meta, pinned, nodes, aliases)
	}
}

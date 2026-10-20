package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// Streaming core: expanding a stored history must not copy the history into
// heap. A ~60MiB, 2000-item turn must expand and accept with a small,
// bounded heap delta independent of history size.
func TestStreamingExpansionHeapBound(t *testing.T) {
	s := newTestStore(t, time.Hour, 256<<20, 64<<20, 32<<20, 4<<20)
	o := [32]byte{1}
	// 2000 items x ~30KiB = ~60MiB stored history.
	var full []json.RawMessage
	for i := 0; i < 2000; i++ {
		full = append(full, json.RawMessage(fmt.Sprintf(`{"type":"message","role":"user","content":[{"type":"input_text","text":"%s"}]}`, strings.Repeat("x", 30<<10))))
	}
	mustReviewPut(t, s, "big", o, "", full)

	_, srv, k := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 80 << 20 })
	_ = srv
	owner := [32]byte{1}
	// Seed the same content under the keeper's own store so expansion reads
	// stored items, not client input.
	k2 := k
	k2.store = s

	var b1, b2 runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&b1)
	raw, err := k2.spoolRequest(context.Background(), strings.NewReader(`{"model":"m","input":"new","previous_response_id":"big"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	turn, apiErr := k2.prepareDisk(context.Background(), raw, owner, func(id string) ([32]byte, bool) {
		tip, _, ok := k2.store.resolveID(id, owner, false)
		return tip, ok
	}, false)
	if apiErr != nil {
		t.Fatalf("prepareDisk: %+v", apiErr)
	}
	defer turn.file.Close()
	runtime.ReadMemStats(&b2)
	delta := int64(b2.TotalAlloc - b1.TotalAlloc)
	if delta > 8<<20 {
		t.Fatalf("expansion allocated %d bytes for a ~60MiB history", delta)
	}
	if len(turn.spans) != 2001 {
		t.Fatalf("spans=%d", len(turn.spans))
	}
	// Byte-exactness: replayed upstream spool decodes to the same items.
	hist, err := turn.history()
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 2001 {
		t.Fatalf("history items=%d", len(hist))
	}
	for i := 0; i < 2000; i++ {
		if string(hist[i]) != string(full[i]) {
			t.Fatalf("item %d differs", i)
		}
	}
}

// Orphans staged by an accept whose link fails are swept; accepted history
// stays valid.
func TestStreamingOrphanSweepOnLinkFailure(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 40, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "keep", o, "", items(`"A"`))
	// pinMax=40 forces errStoreBudget in the final link tx after staging.
	big := json.RawMessage(`"` + strings.Repeat("z", 100) + `"`)
	if _, _, err := s.accept("rejected", o, sliceItems([]json.RawMessage{big}), "", false, true); err != errStoreBudget {
		t.Fatalf("accept err=%v", err)
	}
	// Orphan must have been swept: no refs==0 items remain.
	_ = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketItemRefs).ForEach(func(k, v []byte) error {
			if number(v[8:]) == 0 {
				t.Fatalf("orphan item %x", k)
			}
			return nil
		})
	})
	roundtrip(t, s, "keep", o, []string{`"A"`})
	if _, ok := s.get("rejected", o); ok {
		t.Fatal("rejected alias readable")
	}
	inspectStore(t, s)
}

// Staged items survive only while their link tx commits; a staged item whose
// write fails mid-batch leaves the store consistent.
func TestStreamingStagedItemFailureLeavesNoDanglingNode(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "base", o, "", items(`"P"`))
	boom := errors.New("injected")
	failing := func() itemSource {
		i := 0
		return func([]byte) ([]byte, error) {
			i++
			if i == 2 {
				return nil, boom
			}
			return json.RawMessage(fmt.Sprintf(`"I%d"`, i)), nil
		}
	}
	if _, _, err := s.accept("x", o, failing, "", true, false); err == nil {
		t.Fatal("accept succeeded despite injected failure")
	}
	roundtrip(t, s, "base", o, []string{`"P"`})
	if _, ok := s.get("x", o); ok {
		t.Fatal("failed alias readable")
	}
	inspectStore(t, s)
}

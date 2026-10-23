package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func newTestStore(t *testing.T, ttl time.Duration, maxLive, maxMeta, pinMax, compactAt int64) *store {
	t.Helper()
	s, err := newStore(t.TempDir(), ttl, maxLive, maxMeta, pinMax, compactAt)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	return s
}
func items(strs ...string) []json.RawMessage {
	out := make([]json.RawMessage, len(strs))
	for i, s := range strs {
		out[i] = json.RawMessage(s)
	}
	return out
}
func roundtrip(t *testing.T, s *store, id string, owner [32]byte, want []string) {
	t.Helper()
	got, ok := s.get(id, owner)
	if !ok {
		t.Fatalf("get %s: missing", id)
	}
	checkItems(t, got, want)
}
func checkItems(t *testing.T, got []json.RawMessage, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("items=%d want=%d", len(got), len(want))
	}
	for i := range got {
		if !json.Valid(got[i]) || string(got[i]) != want[i] {
			t.Fatalf("item %d=%s want=%s", i, got[i], want[i])
		}
	}
}

// Derive charges from actual records, independently of persisted counters.
// Also verify every node/alias/pin reference and item occurrence on disk.
func inspectStore(t *testing.T, s *store) (live, meta, pinned int64, nodes, aliases int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.db.View(func(tx *bolt.Tx) error {
		refs := map[string]int64{}
		itemRefs := map[string]int64{}
		var zero [32]byte
		_ = tx.Bucket(bucketNodes).ForEach(func(k, v []byte) error {
			nodes++
			meta += nodeMetaOverhead
			if string(v[:32]) != string(zero[:]) {
				refs[string(v[:32])]++
			}
			if string(v[32:64]) != string(zero[:]) {
				itemRefs[string(v[32:64])]++
			}
			return nil
		})
		_ = tx.Bucket(bucketResponses).ForEach(func(k, v []byte) error {
			aliases++
			meta += respMetaOverhead + int64(len(k))
			refs[string(v[:32])]++
			return nil
		})
		_ = tx.Bucket(bucketPins).ForEach(func(k, v []byte) error {
			meta += pinMetaOverhead
			pinned += number(v[32:])
			refs[string(v[:32])]++
			return nil
		})
		_ = tx.Bucket(bucketNodes).ForEach(func(k, v []byte) error {
			if refs[string(k)] != number(v[96:]) || refs[string(k)] == 0 {
				t.Errorf("node reference mismatch: %x", k)
			}
			delete(refs, string(k))
			return nil
		})
		if len(refs) > 0 {
			t.Error("dangling node references")
		}
		_ = tx.Bucket(bucketItems).ForEach(func(k, v []byte) error {
			if !json.Valid(v) {
				t.Errorf("item payload is not valid JSON: %x", k)
			}
			if int64(len(v)) != number(tx.Bucket(bucketItemRefs).Get(k)[:8]) {
				t.Errorf("item payload length disagrees with refs record: %x", k)
			}
			return nil
		})
		_ = tx.Bucket(bucketItemRefs).ForEach(func(k, v []byte) error {
			live += number(v[:8])
			meta += hashMetaOverhead
			if itemRefs[string(k)] != number(v[8:]) || number(v[8:]) == 0 {
				t.Errorf("item reference/content mismatch: %x", k)
			}
			if tx.Bucket(bucketItems).Get(k) == nil {
				t.Errorf("refs record without payload: %x", k)
			}
			delete(itemRefs, string(k))
			return nil
		})
		if len(itemRefs) > 0 {
			t.Error("dangling item references")
		}
		b := tx.Bucket(bucketCounts)
		if live != number(b.Get([]byte("live"))) || meta != number(b.Get([]byte("meta"))) || pinned != number(b.Get([]byte("pinned"))) {
			t.Errorf("record charges %d/%d/%d disagree with counters", live, meta, pinned)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if (s.maxLive > 0 && live > s.maxLive) || meta > s.maxMeta || pinned > s.pinMax {
		t.Fatalf("budgets exceeded: %d/%d/%d", live, meta, pinned)
	}
	return
}
func dropTestResponse(s *store, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.update(func(st *storeTx) error { return st.dropResponse([]byte(id)) })
}
func expireTestStore(s *store, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.update(func(st *storeTx) error { return st.expire(now) })
}
func mustReviewPut(t *testing.T, s *store, id string, o [32]byte, prev string, full []json.RawMessage) string {
	t.Helper()
	n, err := s.put(id, o, prev, full)
	if err != nil {
		t.Fatal(err)
	}
	return n
}
func TestStoreDedupIdenticalContent(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	for _, id := range []string{"r1", "r2"} {
		mustReviewPut(t, s, id, o, "", items(`"AAAA"`))
		roundtrip(t, s, id, o, []string{`"AAAA"`})
	}
	if live, _, _, _, _ := inspectStore(t, s); live != 6 {
		t.Fatalf("live=%d", live)
	}
}
func TestStoreSharedPrefixChaining(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "r1", o, "", items(`"P1"`, `"P2"`))
	mustReviewPut(t, s, "r2", o, "r1", items(`"P1"`, `"P2"`, `"N1"`))
	if err := dropTestResponse(s, "r1"); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.get("r1", o); ok {
		t.Fatal("removed alias still readable")
	}
	roundtrip(t, s, "r2", o, []string{`"P1"`, `"P2"`, `"N1"`})
	if live, _, _, nodes, _ := inspectStore(t, s); live != 12 || nodes != 4 {
		t.Fatalf("live=%d nodes=%d", live, nodes)
	}
}
func TestStoreGrowingHistoryLinear(t *testing.T) {
	for _, fallback := range []bool{false, true} {
		t.Run(fmt.Sprint("fallback=", fallback), func(t *testing.T) {
			s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
			o := [32]byte{1}
			prev := ""
			var hist []string
			var unique int64
			for turn := 0; turn < 80; turn++ {
				item := fmt.Sprintf(`{"turn":%d,"pad":"0123456789"}`, turn)
				hist = append(hist, item)
				unique += int64(len(item))
				id := fmt.Sprint("r", turn)
				if fallback {
					prev = ""
				}
				mustReviewPut(t, s, id, o, prev, items(hist...))
				prev = id
				roundtrip(t, s, id, o, hist)
			}
			live, meta, _, nodes, aliases := inspectStore(t, s)
			if live != unique || nodes != 81 || aliases != 80 || meta > 80*600 {
				t.Fatalf("not linear: live=%d nodes=%d aliases=%d meta=%d", live, nodes, aliases, meta)
			}
		})
	}
}
func TestStorePrefixMismatchStandalone(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "r1", o, "", items(`"A"`))
	mustReviewPut(t, s, "r2", o, "r1", items(`"B"`))
	roundtrip(t, s, "r1", o, []string{`"A"`})
	roundtrip(t, s, "r2", o, []string{`"B"`})
	inspectStore(t, s)
}
func TestStoreOwnerIsolationOnReuse(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	a, b := [32]byte{1}, [32]byte{2}
	mustReviewPut(t, s, "shared", a, "", items(`"A"`))
	if _, err := s.put("shared", b, "", items(`"B"`)); err != errStoreOwner {
		t.Fatalf("overwrite err=%v", err)
	}
	roundtrip(t, s, "shared", a, []string{`"A"`})
	if _, ok := s.get("shared", b); ok {
		t.Fatal("foreign read")
	}
}
func TestStorePins(t *testing.T) {
	s := newTestStore(t, time.Minute, 1<<20, 1<<20, 40, 4<<20)
	o := [32]byte{1}
	pin, release, err := s.putPinned(o, "", "", items(`"11112222"`))
	if err != nil {
		t.Fatal(err)
	}
	future := time.Now().Add(2 * time.Minute)
	s.now = func() time.Time { return future }
	if err := expireTestStore(s, future); err != nil {
		t.Fatal(err)
	}
	got, ok := s.getPinned(pin, o)
	if !ok {
		t.Fatal("pin expired")
	}
	checkItems(t, got, []string{`"11112222"`})
	if _, ok := s.get(pin, o); ok {
		t.Fatal("pin registered")
	}
	if _, ok := s.getPinned(pin, [32]byte{2}); ok {
		t.Fatal("foreign pin")
	}
	release()
	release()
	if _, ok := s.getPinned(pin, o); ok {
		t.Fatal("released pin readable")
	}
	raw, _ := json.Marshal(strings.Repeat("X", 50))
	if n, rel, err := s.putPinned(o, "", "", []json.RawMessage{raw}); err == nil || n != "" {
		rel()
		t.Fatal("oversized pin accepted")
	}
	inspectStore(t, s)
}
func TestStoreRestartWipesFile(t *testing.T) {
	dir := t.TempDir()
	s, err := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	o := [32]byte{1}
	mustReviewPut(t, s, "r1", o, "", items(`"A"`))
	if err := s.close(); err != nil {
		t.Fatal(err)
	}
	s, err = newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.close()
	if _, ok := s.get("r1", o); ok {
		t.Fatal("cache survived restart")
	}
	if n, size := s.stats(); n != 0 || size != 0 {
		t.Fatalf("restart stats=%d/%d", n, size)
	}
	inspectStore(t, s)
}
func TestStoreCompactAfterChurn(t *testing.T) {
	s := newTestStore(t, time.Minute, 1<<20, 1<<20, 1<<19, 32<<10)
	o := [32]byte{1}
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }
	for cycle := 0; cycle < 10; cycle++ {
		raw, _ := json.Marshal(strings.Repeat(fmt.Sprint(cycle), 128<<10))
		id := fmt.Sprint("c", cycle)
		mustReviewPut(t, s, id, o, "", []json.RawMessage{raw})
		roundtrip(t, s, id, o, []string{string(raw)})
		now = now.Add(2 * time.Minute)
	}
	before := s.diskUsage()
	mustReviewPut(t, s, "late", o, "", items(`"Z"`))
	if after := s.diskUsage(); after >= before || after > 3*s.compactAt {
		t.Fatalf("no reclamation: before=%d after=%d", before, after)
	}
	roundtrip(t, s, "late", o, []string{`"Z"`})
	inspectStore(t, s)
}
func TestStoreChainVerification(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "r1", o, "", items(`"A"`, `"B"`, `"C"`))
	mustReviewPut(t, s, "r2", o, "r1", items(`"A"`, `"B"`, `"C"`, `"D"`))
	if _, _, _, nodes, _ := inspectStore(t, s); nodes != 5 {
		t.Fatalf("prefix nodes=%d want root+4", nodes)
	}
	mustReviewPut(t, s, "r3", o, "r2", items(`"X"`))
	roundtrip(t, s, "r3", o, []string{`"X"`})
	roundtrip(t, s, "r2", o, []string{`"A"`, `"B"`, `"C"`, `"D"`})
}
func TestStoreDiskUsage(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	mustReviewPut(t, s, "r1", [32]byte{1}, "", items(`"AAAA"`))
	fi, err := os.Stat(filepath.Join(s.dir, "keeper.db"))
	if err != nil {
		t.Fatal(err)
	}
	if s.diskUsage() != fi.Size() {
		t.Fatal("physical usage mismatch")
	}
}

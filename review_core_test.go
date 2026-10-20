package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func reviewUniqueFactory(n, size int) itemFactory {
	return func() itemSource {
		i := 0
		return func(dst []byte) ([]byte, error) {
			if i == n {
				return nil, io.EOF
			}
			if cap(dst) < size {
				dst = make([]byte, size)
			}
			dst = dst[:size]
			for j := range dst {
				dst[j] = 'x'
			}
			const prefix = `{"type":"message","role":"user","content":"`
			copy(dst, prefix)
			dst[size-2] = '"'
			dst[size-1] = '}'
			copy(dst[len(prefix):], fmt.Sprintf("%08x", i))
			i++
			return dst, nil
		}
	}
}

// The final metadata-only transaction must not retain the payload history.
func TestReviewCoreFinalLinkRetainedPayload(t *testing.T) {
	for _, count := range []int{32, 192} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			s := newTestStore(t, time.Hour, 128<<20, 8<<20, 64<<20, 512<<20)
			factory := reviewUniqueFactory(count, 128<<10)
			owner := [32]byte{2}
			plan, err := s.planChain(owner, factory())
			if err != nil {
				t.Fatal(err)
			}
			if err = s.writeNewItems(factory(), plan.newItems); err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			var delta uint64
			err = s.update(func(st *storeTx) error {
				if err := st.linkPlan(owner, plan, s.maxMeta); err != nil {
					return err
				}
				if err := st.ref(plan.tip[:], 1); err != nil {
					return err
				}
				runtime.GC()
				runtime.ReadMemStats(&after)
				if after.HeapAlloc > before.HeapAlloc {
					delta = after.HeapAlloc - before.HeapAlloc
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("payload=%d final-link-live-heap=%d final-link-TotalAlloc=%d", count*(128<<10), delta, after.TotalAlloc-before.TotalAlloc)
			if delta > 8<<20 {
				t.Fatalf("final supposedly metadata-only transaction retains %d heap bytes for %d payload bytes", delta, count*(128<<10))
			}
		})
	}
}

// Same staging/final-link path as HTTP and WS acceptDisk, with a real spool.
// Supervisor direction: the bound that matters is LIVE heap, not cumulative
// TotalAlloc — bbolt's per-batch page buffers are transient and GC-reclaimed.
// Assert post-accept live heap stays ~metadata-sized as unique history grows.
func TestReviewCoreAcceptDiskHeapBound(t *testing.T) {
	_, _, k := newTestKeeper(t, func(c *config) {
		c.maxStoreBytes = 128 << 20
		c.storeMetaBytes = 8 << 20
		c.storePinBytes = 64 << 20
		c.storeCompactBytes = 512 << 20
	})
	measure := func(n int, id string) uint64 {
		f, err := k.newSpool()
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		turn := &diskTurn{file: f, store: true}
		next := reviewUniqueFactory(n, 128<<10)()
		buf := make([]byte, 0, 128<<10)
		for {
			b, e := next(buf)
			if e == io.EOF {
				break
			}
			if e != nil {
				t.Fatal(e)
			}
			off := f.size
			if _, e = f.Write(b); e != nil {
				t.Fatal(e)
			}
			turn.spans = append(turn.spans, itemSpan{off, len(b)})
			buf = b
		}
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		_, err = k.acceptDisk(context.Background(), turn, id, [32]byte{1}, nil, "", false)
		runtime.GC()
		runtime.ReadMemStats(&after)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("acceptDisk unique payload=%d post-GC-live-heap=%d", n*(128<<10), after.HeapAlloc-before.HeapAlloc)
		var delta uint64
		if after.HeapAlloc > before.HeapAlloc {
			delta = after.HeapAlloc - before.HeapAlloc
		}
		return delta
	}
	// 8MiB and 24MiB unique histories: live heap must stay ~metadata, not scale
	// with payload. Both under 8MiB.
	for _, c := range []struct {
		n  int
		id string
	}{{64, "u-8m"}, {192, "u-24m"}} {
		if delta := measure(c.n, c.id); delta > 8<<20 {
			t.Fatalf("%d unique items: accept retains %d live heap bytes", c.n, delta)
		}
	}
}

func TestReviewCoreAcceptedHistoryMustFitReplay(t *testing.T) {
	_, _, k := newTestKeeper(t, func(c *config) {
		c.maxStoreBytes = 256 << 20
		c.storeMetaBytes = 8 << 20
		c.storePinBytes = 128 << 20
		c.storeCompactBytes = 512 << 20
	})
	owner := [32]byte{7}
	one := json.RawMessage(`"` + strings.Repeat("a", (256<<10)-1026) + `"`)
	full := make([]json.RawMessage, 256)
	for i := range full {
		full[i] = one
	}
	if _, err := k.store.put("seed", owner, "", full); err != nil {
		t.Fatal(err)
	}
	prep := func(id string) (*diskTurn, *apiError) {
		raw, err := k.spoolRequest(context.Background(), strings.NewReader(`{"model":"m","input":[],"previous_response_id":"`+id+`"}`))
		if err != nil {
			t.Fatal(err)
		}
		defer raw.Close()
		return k.prepareDisk(context.Background(), raw, owner, func(id string) ([32]byte, bool) { tip, _, ok := k.store.resolveID(id, owner, false); return tip, ok }, false)
	}
	turn, ae := prep("seed")
	if ae != nil {
		t.Fatalf("initial expansion %+v", ae)
	}
	defer turn.file.Close()
	output := json.RawMessage(`"` + strings.Repeat("b", 512<<10) + `"`)
	beforeSize := turn.file.size
	_, err := k.acceptDisk(context.Background(), turn, "too-large-accepted", owner, []json.RawMessage{output}, "", false)
	t.Logf("input spool=%d appended output=%d final spool=%d accept err=%v", beforeSize, len(output), turn.file.size, err)
	if err != nil {
		return
	}
	tip, _, ok := k.store.resolveID("too-large-accepted", owner, false)
	if !ok {
		t.Fatal("no alias after successful accept")
	}
	_ = tip
	next, ae := prep("too-large-accepted")
	if next != nil {
		next.file.Close()
	}
	t.Logf("accepted alias continuation result=%+v", ae)
	if ae != nil {
		t.Fatalf("accept returned success/published alias but its history cannot replay: %+v", ae)
	}
}

// During a failed staging pass, an overlapping successful accept must wait
// for the failing transaction's cleanup, never have its pending data swept.
func TestReviewCoreOverlappingStageFailure(t *testing.T) {
	s := newTestStore(t, time.Hour, 128<<20, 8<<20, 64<<20, 512<<20)
	owner := [32]byte{8}
	if _, err := s.put("base", owner, "", items(`"preserve"`)); err != nil {
		t.Fatal(err)
	}
	var passes atomic.Int32
	staged := make(chan struct{})
	unblock := make(chan struct{})
	boom := errors.New("review stage failure AFTER committed batch")
	failedFactory := func() itemSource {
		pass := passes.Add(1)
		i := 0
		normal := reviewUniqueFactory(80, 64<<10)()
		return func(buf []byte) ([]byte, error) {
			if pass == 2 && i == 65 {
				close(staged)
				<-unblock
				return nil, boom
			}
			i++
			return normal(buf)
		}
	}
	failed := make(chan error, 1)
	go func() { _, _, err := s.accept("failed", owner, failedFactory, "", true, false); failed <- err }()
	select {
	case <-staged:
	case <-time.After(5 * time.Second):
		t.Fatal("no committed-stage checkpoint")
	}
	// A t.Fatal below must not leave the blocked accept holding store.mu.
	defer func() {
		select {
		case <-unblock:
		default:
			close(unblock)
		}
	}()
	// Check the independently committed 4MiB staging batch really exists.
	// Refcounts live in the small itemRefs records: {payloadLen:8, refs:8}.
	var stagedCount int
	if err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketItemRefs).ForEach(func(k, v []byte) error {
			if number(v[8:16]) == 0 {
				stagedCount++
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
	t.Logf("pending rows before injected failure=%d", stagedCount)
	if stagedCount < 64 {
		t.Fatal("fixture failed to commit first staged batch")
	}
	done := make(chan error, 1)
	go func() {
		_, _, err := s.accept("overlap", owner, reviewUniqueFactory(2, 64<<10), "", true, false)
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("overlapping accept entered locked staging window: %v", err)
	case <-time.After(40 * time.Millisecond):
	}
	close(unblock)
	if err := <-failed; err != boom {
		t.Fatalf("failed err=%v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("successful overlap: %v", err)
	}
	roundtrip(t, s, "base", owner, []string{`"preserve"`})
	if _, ok := s.get("failed", owner); ok {
		t.Fatal("failed alias published")
	}
	got, ok := s.get("overlap", owner)
	if !ok || len(got) != 2 {
		t.Fatal("overlap lost")
	}
	inspectStore(t, s)
}

func TestReviewCoreByteExactNontrivialSpans(t *testing.T) {
	_, _, k := newTestKeeper(t)
	owner := sha256.Sum256([]byte("owner"))
	rawText := "{\"model\":\"m\",\"input\": [ \n  { \"type\": \"message\", \"content\": \"<>&\\u0041\" }, \n [1,  2],  \"str\\n\"   ] }"
	raw, err := k.spoolRequest(context.Background(), strings.NewReader(rawText))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	turn, ae := k.prepareDisk(context.Background(), raw, owner, func(string) ([32]byte, bool) { return [32]byte{}, false }, false)
	if ae != nil {
		t.Fatal(ae)
	}
	defer turn.file.Close()
	got, err := turn.history()
	if err != nil {
		t.Fatal(err)
	}
	wants := [][]byte{[]byte(`{ "type": "message", "content": "<>&\u0041" }`), []byte(`[1,  2]`), []byte(`"str\n"`)}
	if len(got) != len(wants) {
		t.Fatal(len(got))
	}
	for i := range got {
		if !bytes.Equal(got[i], wants[i]) {
			t.Fatalf("span %d = %q want %q", i, got[i], wants[i])
		}
	}
}

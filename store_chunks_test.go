package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

// bigItem is a valid JSON item of exactly n bytes, generated lazily.
type bigItem struct{ off, n int64 }

const bigPrefix, bigSuffix = `{"type":"message","content":"`, `"}`

func (b *bigItem) Read(p []byte) (int, error) {
	if b.off >= b.n {
		return 0, io.EOF
	}
	i := 0
	for ; i < len(p) && b.off < b.n; i, b.off = i+1, b.off+1 {
		switch {
		case b.off < int64(len(bigPrefix)):
			p[i] = bigPrefix[b.off]
		case b.off >= b.n-int64(len(bigSuffix)):
			p[i] = bigSuffix[b.off-(b.n-int64(len(bigSuffix)))]
		default:
			p[i] = 'a' + byte(b.off%26)
		}
	}
	return i, nil
}

func bigBytes(n int64) []byte { b, _ := io.ReadAll(&bigItem{n: n}); return b }

// bigStream yields small, big(n), small, big(n) again (a duplicate).
func bigStream(n int64) itemStreamFactory {
	return func() itemStream {
		i := 0
		return func() (int64, io.Reader, error) {
			i++
			switch i {
			case 1, 3:
				s := `{"type":"message","content":"small"}`
				return int64(len(s)), strings.NewReader(s), nil
			case 2, 4:
				return n, &bigItem{n: n}, nil
			}
			return 0, nil, io.EOF
		}
	}
}

func chunkCount(t *testing.T, s *store) (chunks int, single int) {
	t.Helper()
	_ = s.view(func(tx *bolt.Tx) error {
		chunks = tx.Bucket(bucketItemChunks).Stats().KeyN
		single = tx.Bucket(bucketItems).Stats().KeyN
		return nil
	})
	return
}

// A large item is stored as ordered bounded chunks, read back by streaming,
// and its chunks are deleted with it.
func TestStoreChunksLargeItem(t *testing.T) {
	s := newTestStore(t, time.Hour, 0, 8<<20, 64<<20, 512<<20)
	const n = 5*itemChunkBytes + 12345
	owner := [32]byte{7}
	tip, _, err := s.acceptStream(context.Background(), "big", owner, bigStream(n), "", true, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if chunks, single := chunkCount(t, s); chunks != 6 || single != 1 {
		t.Fatalf("chunks=%d single=%d; want 6 chunks and 1 small item", chunks, single)
	}
	inspectStore(t, s)
	want := bigBytes(n)
	got, ok := s.get("big", owner)
	if !ok || len(got) != 4 || !bytes.Equal(got[1], want) || !bytes.Equal(got[3], want) {
		t.Fatalf("get: ok=%v items=%d", ok, len(got))
	}
	var tipID [32]byte
	copy(tipID[:], tip)
	h, err := s.snapshotTip(context.Background(), tipID, owner)
	if err != nil {
		t.Fatal(err)
	}
	var streamed [][]byte
	err = h.each(context.Background(), func(size int64, r io.Reader) error {
		b, err := io.ReadAll(r)
		if int64(len(b)) != size {
			t.Errorf("streamed %d of %d bytes", len(b), size)
		}
		streamed = append(streamed, b)
		return err
	})
	h.Close()
	if err != nil || len(streamed) != 4 || !bytes.Equal(streamed[1], want) {
		t.Fatalf("each: %v items=%d", err, len(streamed))
	}
	if err := s.forget("big"); err != nil {
		t.Fatal(err)
	}
	if chunks, single := chunkCount(t, s); chunks != 0 || single != 0 {
		t.Fatalf("after forget chunks=%d single=%d", chunks, single)
	}
	inspectStore(t, s)
}

// Migration: a large item written as one value (the pre-chunk format) stays
// readable unchanged, is linked by a new turn, and is deleted with it.
func TestStoreLegacySingleValueItemReadable(t *testing.T) {
	s := newTestStore(t, time.Hour, 0, 8<<20, 64<<20, 512<<20)
	owner := [32]byte{8}
	legacy := json.RawMessage(bigBytes(3*itemChunkBytes + 7))
	full := []json.RawMessage{legacy, json.RawMessage(`{"type":"message","content":"next"}`)}
	plan, err := s.planChain(owner, sliceItems(full)())
	if err != nil {
		t.Fatal(err)
	}
	if err := s.writeNewItems(sliceItems(full)(), plan.newItems); err != nil { // the old single-Put writer
		t.Fatal(err)
	}
	if chunks, single := chunkCount(t, s); chunks != 0 || single != 2 {
		t.Fatalf("legacy setup chunks=%d single=%d", chunks, single)
	}
	if _, err := s.put("legacy", owner, "", full); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(legacy)
	_ = s.view(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketItems).Get(hash[:]); !bytes.Equal(v, legacy) {
			t.Error("legacy record rewritten")
		}
		return nil
	})
	got, ok := s.get("legacy", owner)
	if !ok || len(got) != 2 || !bytes.Equal(got[0], legacy) {
		t.Fatalf("legacy get ok=%v", ok)
	}
	inspectStore(t, s)
	if err := s.forget("legacy"); err != nil {
		t.Fatal(err)
	}
	if chunks, single := chunkCount(t, s); chunks != 0 || single != 0 {
		t.Fatalf("after forget chunks=%d single=%d", chunks, single)
	}
}

// Storing a large item never holds it in heap: preparing streams it, and
// each bounded write allocates within storeWriteCredit (one 51 MiB Put
// allocated about 160 MiB in bbolt before chunking).
func TestStoreLargeItemHeapBounded(t *testing.T) {
	s := newTestStore(t, time.Hour, 0, 8<<20, 256<<20, 1<<30)
	const n = 51 << 20
	var prev, worst uint64
	var ms runtime.MemStats
	s.stageCommitted = func() error {
		runtime.ReadMemStats(&ms)
		worst = max(worst, ms.TotalAlloc-prev)
		prev = ms.TotalAlloc
		return nil
	}
	runtime.ReadMemStats(&ms)
	start := ms.TotalAlloc
	p, err := s.prepareStoreSource(context.Background(), [32]byte{9}, bigStream(n))
	if err != nil {
		t.Fatal(err)
	}
	runtime.ReadMemStats(&ms)
	if d := ms.TotalAlloc - start; d > 4<<20 {
		t.Fatalf("prepare allocated %d bytes for a %d-byte item", d, n)
	}
	p.Close()
	prev = ms.TotalAlloc
	if _, _, err := s.acceptStream(context.Background(), "big", [32]byte{9}, bigStream(n), "", true, false, nil); err != nil {
		t.Fatal(err)
	}
	t.Logf("worst write-tx allocation %d bytes (credit %d)", worst, storeWriteCredit)
	if worst == 0 || worst > uint64(storeWriteCredit) {
		t.Fatalf("one write tx allocated %d bytes; credit %d", worst, storeWriteCredit)
	}
}

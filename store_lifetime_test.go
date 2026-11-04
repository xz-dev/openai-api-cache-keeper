package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func waitResult(t *testing.T, result <-chan error) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("storage operation did not finish")
	}
}

func TestLifetimeSnapshotSurvivesEvictionAndGenerationSwap(t *testing.T) {
	s := newTestStore(t, time.Hour, 16<<20, 1<<20, 8<<20, 32<<20)
	owner := [32]byte{21}
	full := items(`"first"`, `{"text":"second"}`)
	if _, err := s.put("original", owner, "", full); err != nil {
		t.Fatal(err)
	}
	history, err := s.snapshotID(context.Background(), "original", owner, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = history.Close() })
	if _, err := s.snapshotID(context.Background(), "original", [32]byte{22}, false); !errors.Is(err, errMissingItem) {
		t.Fatalf("foreign snapshot accepted: %v", err)
	}
	if err := s.forget("original"); err != nil {
		t.Fatal(err)
	}
	compacted := make(chan error, 1)
	go func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		compacted <- s.compactLocked()
	}()
	waitResult(t, compacted)
	if s.current == history.gen {
		t.Fatal("compaction did not publish a new generation")
	}
	select {
	case <-history.gen.done:
		t.Fatal("old generation closed while history reader owned it")
	default:
	}
	var got []json.RawMessage
	if err := history.each(context.Background(), func(_ int64, r io.Reader) error {
		data, err := io.ReadAll(r)
		got = append(got, data)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(full) || !bytes.Equal(got[0], full[0]) || !bytes.Equal(got[1], full[1]) {
		t.Fatalf("snapshot torn: %q", got)
	}
	if err := history.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-history.gen.done:
	case <-time.After(time.Second):
		t.Fatal("retired generation did not drain")
	}
	entries, live := s.stats()
	if entries != 0 || live != 0 {
		t.Fatalf("snapshot lease leaked content: entries=%d live=%d", entries, live)
	}
	inspectStore(t, s)
}

func TestLifetimeSnapshotSlowOutputDoesNotHoldMmapTransaction(t *testing.T) {
	s := newTestStore(t, time.Hour, 32<<20, 2<<20, 16<<20, 64<<20)
	owner := [32]byte{31}
	payload := json.RawMessage(`"` + strings.Repeat("a", 256<<10) + `"`)
	if _, err := s.put("snapshot", owner, "", []json.RawMessage{payload}); err != nil {
		t.Fatal(err)
	}
	// Reopen with the default mapping size so the following multi-megabyte
	// commit must remap, rather than fit the production 128-MiB initial mmap.
	s.mu.Lock()
	if err := s.db.Close(); err != nil {
		s.mu.Unlock()
		t.Fatal(err)
	}
	db, err := bolt.Open(s.path, 0600, boltOptions())
	if err == nil {
		s.publishGeneration(db)
	}
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	before := fileSizeOf(s.path)
	history, err := s.snapshotID(context.Background(), "snapshot", owner, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = history.Close() })
	paused, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	result := make(chan error, 1)
	var workers sync.WaitGroup
	t.Cleanup(func() { once.Do(func() { close(release) }); workers.Wait() })
	workers.Add(1)
	go func() {
		defer workers.Done()
		result <- history.each(context.Background(), func(_ int64, r io.Reader) error {
			first := make([]byte, 32<<10)
			if _, err := io.ReadFull(r, first); err != nil {
				return err
			}
			close(paused)
			<-release
			rest, err := io.ReadAll(r)
			if err == nil && !bytes.Equal(append(first, rest...), payload) {
				err = errors.New("history changed during remap")
			}
			return err
		})
	}()
	select {
	case <-paused:
	case <-time.After(time.Second):
		t.Fatal("history did not pause outside its read transaction")
	}
	write := make(chan error, 1)
	workers.Add(1)
	go func() {
		defer workers.Done()
		_, err := s.put("remap", owner, "", []json.RawMessage{json.RawMessage(`"` + strings.Repeat("b", 8<<20) + `"`)})
		write <- err
	}()
	waitResult(t, write)
	if fileSizeOf(s.path) <= 4*before {
		t.Fatal("fixture did not grow beyond its old mmap window")
	}
	once.Do(func() { close(release) })
	waitResult(t, result)
}

func TestLifetimeSlowAcceptAndCompactionDoNotBlockNoStoreHTTP(t *testing.T) {
	for _, stage := range []string{"source", "compact"} {
		t.Run(stage, func(t *testing.T) {
			_, srv, k := newTestKeeper(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			result := make(chan error, 1)
			var workers sync.WaitGroup
			t.Cleanup(func() { once.Do(func() { close(release) }); workers.Wait() })
			workers.Add(1)
			if stage == "compact" {
				k.store.compactCopy = func(dst, src *bolt.DB, limit int64) error {
					close(entered)
					<-release
					return bolt.Compact(dst, src, limit)
				}
				go func() {
					defer workers.Done()
					k.store.mu.Lock()
					defer k.store.mu.Unlock()
					result <- k.store.compactLocked()
				}()
			} else {
				var first sync.Once
				source := func() itemSource {
					next := sliceItems(items(`"slow"`))()
					return func(buf []byte) ([]byte, error) {
						first.Do(func() { close(entered); <-release })
						return next(buf)
					}
				}
				go func() {
					defer workers.Done()
					_, _, err := k.store.acceptContext(context.Background(), "slow", [32]byte{}, source, "", true, false)
					result <- err
				}()
			}
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("slow storage action never entered")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"input":"unrelated","store":false,"stream":false}`))
			resp, err := srv.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body, err := io.ReadAll(resp.Body)
			resp.Body.Close()
			if err != nil || resp.StatusCode != 200 || !strings.Contains(string(body), `"status":"completed"`) {
				t.Fatalf("no-store HTTP outcome waited for %s: %d %v %s", stage, resp.StatusCode, err, body)
			}
			// The handler's final logging must finish before slow storage resumes.
			lookup := make(chan error, 1)
			go func() {
				k.store.resolveID("missing", [32]byte{}, false)
				k.store.ownedID("missing", [32]byte{})
				k.cacheMu.Lock()
				k.cacheMu.Unlock()
				lookup <- nil
			}()
			waitResult(t, lookup)
			once.Do(func() { close(release) })
			waitResult(t, result)
		})
	}
}

func TestLifetimeQueuedMutationCancellationCannotPublish(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	s.mu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, _, err := s.acceptContext(ctx, "cancelled", [32]byte{}, sliceItems(items(`"payload"`)), "", true, false)
	s.mu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("queued mutation did not cancel: %v", err)
	}
	if _, _, found := s.resolveID("cancelled", [32]byte{}, false); found {
		t.Fatal("cancelled queued mutation published an alias")
	}
	inspectStore(t, s)
}

func TestLifetimeCloseRejectsNewReadersAndDrainsBeforeDirectoryUnlock(t *testing.T) {
	dir := t.TempDir()
	s, err := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	generation, err := s.acquireGeneration()
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	var workers sync.WaitGroup
	t.Cleanup(func() { once.Do(func() { s.releaseGeneration(generation) }); workers.Wait() })
	closed := make(chan error, 1)
	workers.Add(1)
	go func() { defer workers.Done(); closed <- s.close() }()
	deadline := time.Now().Add(time.Second)
	for {
		s.generationMu.Lock()
		closing := s.closed
		s.generationMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close never rejected new readers")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := s.acquireGeneration(); !errors.Is(err, errStoreClosed) {
		t.Fatalf("store accepted a reader during close: %v", err)
	}
	if other, err := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20); err == nil {
		other.close()
		t.Fatal("directory lock released before generation drained")
	}
	once.Do(func() { s.releaseGeneration(generation) })
	waitResult(t, closed)
	other, err := newStore(dir, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	other.close()
}

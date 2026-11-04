package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Pause after a real payload commit, with no bbolt transaction still open.
func pauseFirstStage(s *store) (<-chan struct{}, func()) {
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	var once sync.Once
	s.stageCommitted = func() error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return nil
	}
	return entered, func() { once.Do(func() { close(release) }) }
}

func awaitStage(t *testing.T, entered <-chan struct{}) {
	t.Helper()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("payload stage did not commit")
	}
}

func TestStagingIndependentStoredHTTP(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	entered, release := pauseFirstStage(k.store)
	done := make(chan error, 1)
	go func() {
		_, _, err := k.store.accept("staging-A", [32]byte{}, sliceItems(items(`"A"`)), "", true, false)
		done <- err
	}()
	defer func() { release(); waitResult(t, done) }()
	awaitStage(t, entered)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"input":"tiny","store":true,"stream":false}`))
	res, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(res.Body)
	res.Body.Close()
	if err != nil || res.StatusCode != 200 {
		t.Fatalf("B failed while A paused: status=%d err=%v body=%s", res.StatusCode, err, body)
	}
	if entries, _ := k.store.stats(); entries != 1 {
		t.Fatalf("B was not stored before releasing A: entries=%d", entries)
	}
	if _, _, ok := k.store.resolveID("staging-A", [32]byte{}, false); ok {
		t.Fatal("A linked before stage was released")
	}
}

func TestStagingSurvivesOtherFailureAndSweep(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	owner := [32]byte{1}
	if _, err := s.put("taken", owner, "", items(`"old"`)); err != nil {
		t.Fatal(err)
	}
	entered, release := pauseFirstStage(s)
	done := make(chan error, 1)
	go func() {
		_, _, err := s.accept("A", owner, sliceItems(items(`"shared"`)), "", true, false)
		done <- err
	}()
	defer release()
	awaitStage(t, entered)
	if _, _, err := s.accept("taken", [32]byte{2}, sliceItems(items(`"shared"`, `"orphan"`)), "", true, false); !errors.Is(err, errStoreOwner) {
		t.Fatalf("B should fail owner validation: %v", err)
	}
	s.mu.Lock()
	err := s.update(func(st *storeTx) error { return st.sweepOrphans() })
	s.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	release()
	waitResult(t, done)
	roundtrip(t, s, "A", owner, []string{`"shared"`})
	roundtrip(t, s, "taken", owner, []string{`"old"`})
	inspectStore(t, s)
}

func TestStagingSurvivesEvictionAndGenerationChange(t *testing.T) {
	s := newTestStore(t, time.Hour, 32<<20, 1<<20, 16<<20, 128<<20)
	owner := [32]byte{1}
	first, _ := json.Marshal(strings.Repeat("a", 4<<20))
	second := json.RawMessage(`"next-batch"`)
	if _, err := s.put("old", owner, "", []json.RawMessage{first}); err != nil {
		t.Fatal(err)
	}
	entered, release := pauseFirstStage(s)
	done := make(chan error, 1)
	go func() {
		_, _, err := s.accept("A", owner, sliceItems([]json.RawMessage{first, second}), "", true, false)
		done <- err
	}()
	defer release()
	awaitStage(t, entered)
	if err := s.forget("old"); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	old := s.current
	err := s.compactLocked()
	changed := old != s.current
	s.mu.Unlock()
	if err != nil || !changed {
		t.Fatalf("compaction while A staged: changed=%v err=%v", changed, err)
	}
	if _, err := s.put("B", owner, "", items(`"independent"`)); err != nil {
		t.Fatal(err)
	}
	release()
	waitResult(t, done)
	roundtrip(t, s, "A", owner, []string{string(first), string(second)})
	roundtrip(t, s, "B", owner, []string{`"independent"`})
	inspectStore(t, s)
}

func TestStagingCancellationPreservesAliasPinAndReclaims(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	owner := [32]byte{1}
	_, pin, err := s.accept("A", owner, sliceItems(items(`"old"`)), "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	entered, release := pauseFirstStage(s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := s.acceptContext(ctx, "A", owner, sliceItems(items(`"unlinked"`)), pin, true, true)
		done <- err
	}()
	defer release()
	awaitStage(t, entered)
	cancel()
	release()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not join staging")
	}
	roundtrip(t, s, "A", owner, []string{`"old"`})
	got, ok := s.getPinned(pin, owner)
	if !ok {
		t.Fatal("old pin lost")
	}
	checkItems(t, got, []string{`"old"`})
	inspectStore(t, s) // rejects every zero-reference payload left behind
	s.inputWork.mu.Lock()
	input, disk := s.inputWork.input, s.inputWork.disk
	s.inputWork.mu.Unlock()
	if input != 0 || disk != 0 {
		t.Fatalf("stage ownership leaked: input=%d disk=%d", input, disk)
	}
}

func TestStagingCloseJoinsWithoutMutationDeadlock(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	entered, release := pauseFirstStage(s)
	done := make(chan error, 1)
	go func() {
		_, _, err := s.accept("A", [32]byte{}, sliceItems(items(`"A"`)), "", true, false)
		done <- err
	}()
	defer release()
	awaitStage(t, entered)
	closed := make(chan error, 1)
	go func() { closed <- s.close() }()
	awaitStage(t, s.closing)
	select {
	case <-closed:
		t.Fatal("close escaped active stage generation")
	default:
	}
	release()
	select {
	case err := <-done:
		if !errors.Is(err, errStoreClosed) {
			t.Fatalf("accept after close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("stage cleanup deadlocked behind close")
	}
	waitResult(t, closed)
	s.inputWork.mu.Lock()
	input, disk := s.inputWork.input, s.inputWork.disk
	s.inputWork.mu.Unlock()
	if input != 0 || disk != 0 {
		t.Fatalf("closed stage leaked: input=%d disk=%d", input, disk)
	}
	if s.staging != nil {
		t.Fatal("closed stage owner retained")
	}
}

func TestStagingSharedPayloadReclaimedAfterLastCancelledOwner(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	entered := make(chan struct{}, 2)
	releases := [2]chan struct{}{make(chan struct{}), make(chan struct{})}
	var calls atomic.Int32
	var once [2]sync.Once
	release := func(i int) { once[i].Do(func() { close(releases[i]) }) }
	defer release(0)
	defer release(1)
	s.stageCommitted = func() error {
		i := int(calls.Add(1)) - 1
		entered <- struct{}{}
		<-releases[i]
		return nil
	}
	ctxA, cancelA := context.WithCancel(context.Background())
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelA()
	defer cancelB()
	done := [2]chan error{make(chan error, 1), make(chan error, 1)}
	for i, ctx := range []context.Context{ctxA, ctxB} {
		go func(i int, ctx context.Context) {
			_, _, err := s.acceptContext(ctx, string(rune('A'+i)), [32]byte{}, sliceItems(items(`"shared"`)), "", true, false)
			done[i] <- err
		}(i, ctx)
		awaitStage(t, entered)
	}
	for i, cancel := range []context.CancelFunc{cancelA, cancelB} {
		cancel()
		release(i)
		select {
		case err := <-done[i]:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("owner %d cancellation: %v", i, err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("cancelled owner did not join")
		}
		if i == 0 {
			s.mu.Lock()
			err := s.update(func(st *storeTx) error {
				if err := st.sweepOrphans(); err != nil {
					return err
				}
				if st.tx.Bucket(bucketItems).Stats().KeyN != 1 {
					t.Error("first cancelled owner or sweep deleted second owner's payload")
				}
				return nil
			})
			s.mu.Unlock()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	inspectStore(t, s)
	if entries, live := s.stats(); entries != 0 || live != 0 {
		t.Fatalf("cancelled stages published: entries=%d live=%d", entries, live)
	}
}

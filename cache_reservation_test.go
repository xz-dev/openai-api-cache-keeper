package main

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"
)

func TestLifetimeHistoryReservationAndEventCredits(t *testing.T) {
	_, _, k := newTestKeeper(t, func(cfg *config) { cfg.maxStoreBytes = 1024 })
	log, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	task := &responseTask{keeper: k, log: log}
	t.Cleanup(task.discard)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var worker sync.WaitGroup
	t.Cleanup(func() { once.Do(func() { close(release) }); worker.Wait() })
	source := func() itemSource {
		read := sliceItems(items(`"pending-history"`))()
		return func(buf []byte) ([]byte, error) {
			onceEntered := false
			select {
			case <-entered:
			default:
				onceEntered = true
			}
			if onceEntered {
				close(entered)
				<-release
			}
			return read(buf)
		}
	}
	result := make(chan error, 1)
	worker.Add(1)
	go func() {
		defer worker.Done()
		_, _, err := k.store.acceptWithBudget(ctx, "cancelled-history", [32]byte{}, source, "", true, false, func() (int64, func()) { return k.reserveHistoryCache(32) })
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("history source did not enter")
	}
	task.mu.Lock()
	err = task.append([]byte("fits"))
	if err == nil {
		err = task.append(make([]byte, 1024))
	}
	task.mu.Unlock()
	if !errors.Is(err, errWorkSize) {
		t.Fatalf("event write escaped history reservation: %v", err)
	}
	k.cacheMu.Lock()
	if k.cachePending != 0 || k.eventBytes != 4 || k.cacheHistoryReserve != 0 {
		t.Errorf("incoherent pending credits: event=%d pending=%d history=%d", k.eventBytes, k.cachePending, k.cacheHistoryReserve)
	}
	k.cacheMu.Unlock()
	cancel()
	once.Do(func() { close(release) })
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("in-flight source cancellation failed: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled history writer did not stop")
	}
	if _, _, found := k.store.resolveID("cancelled-history", [32]byte{}, false); found {
		t.Fatal("cancelled source published an alias")
	}
	task.mu.Lock()
	err = task.append([]byte("retained-event"))
	task.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	k.cacheMu.Lock()
	if k.cacheHistoryReserve != 0 || k.cachePending != 0 || k.eventBytes != int64(4+len("retained-event")) {
		t.Errorf("credits did not reconcile: event=%d pending=%d history=%d", k.eventBytes, k.cachePending, k.cacheHistoryReserve)
	}
	k.cacheMu.Unlock()
	task.discard()
	k.cacheMu.Lock()
	if k.eventBytes != 0 || k.cachePending != 0 || k.cacheHistoryReserve != 0 {
		t.Error("cache reservation leaked after discard")
	}
	k.cacheMu.Unlock()
	k.work.mu.Lock()
	if k.work.disk != 0 {
		t.Errorf("spool disk charge leaked: %d", k.work.disk)
	}
	k.work.mu.Unlock()
	if _, err := log.ReadAt(make([]byte, 1), 0); err == nil || err == io.EOF {
		t.Error("discard left the spool readable")
	}
}

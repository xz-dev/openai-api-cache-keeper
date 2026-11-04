package main

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestPreparedStoreSourceCloseDrainsCancelledProducer(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		source := func() itemSource {
			return func([]byte) ([]byte, error) {
				close(entered)
				<-ctx.Done()
				return nil, ctx.Err()
			}
		}
		_, _, err := s.acceptContext(ctx, "cancelled-source", [32]byte{}, source, "", true, false)
		result <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("source did not enter")
	}
	closed := make(chan error, 1)
	go func() { closed <- s.close() }()
	deadline := time.Now().Add(time.Second)
	for {
		s.generationMu.Lock()
		closing := s.closed
		s.generationMu.Unlock()
		if closing {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("close did not reject new work")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-closed:
		t.Fatal("directory ownership ended while source still owned its spool")
	default:
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("source cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled source did not release store ownership")
	}
	waitResult(t, closed)
	s.inputWork.mu.Lock()
	input, disk := s.inputWork.input, s.inputWork.disk
	s.inputWork.mu.Unlock()
	if input != 0 || disk != 0 {
		t.Fatalf("prepared source leaked input/disk: %d/%d", input, disk)
	}
}

func TestPreparedStoreSourcePressureCleansBeforePublication(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	free, err := s.inputWork.reserveInput(maxInputWorkingBytes)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.acceptContext(context.Background(), "capacity", [32]byte{}, func() itemSource {
		return func([]byte) ([]byte, error) { return nil, io.EOF }
	}, "", true, false)
	free()
	if !errors.Is(err, errWorkSize) {
		t.Fatalf("preparation pressure not rejected: %v", err)
	}
	if _, _, found := s.resolveID("capacity", [32]byte{}, false); found {
		t.Fatal("failed preparation published alias")
	}
	s.inputWork.mu.Lock()
	input, disk := s.inputWork.input, s.inputWork.disk
	s.inputWork.mu.Unlock()
	if input != 0 || disk != 0 {
		t.Fatalf("pressure path leaked input/disk: %d/%d", input, disk)
	}
	inspectStore(t, s)
}

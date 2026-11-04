package main

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestConcurrentCommentFramesUseSharedBudget(t *testing.T) {
	m := newWorkingSet()
	body := strings.Repeat(":\n", 200000) + "\n"
	ready := make(chan struct{}, 16)
	release := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			err := readSSE(strings.NewReader(body), func(raw []byte) error {
				if len(raw) != len(body) {
					t.Error("comment frame altered")
				}
				ready <- struct{}{}
				<-release
				return nil
			}, func([]byte) error { t.Error("comment interpreted as data"); return nil }, m)
			if err != nil {
				t.Errorf("bounded comment frame: %v", err)
			}
		})
	}
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	count := 0
	for count < 16 {
		select {
		case <-ready:
			count++
		case <-timer.C:
			close(release)
			wg.Wait()
			t.Fatalf("only %d callbacks arrived", count)
		}
	}
	m.mu.Lock()
	charge := m.output
	m.mu.Unlock()
	if charge < int64(16*len(body)) || charge > maxCollectedBytes {
		t.Errorf("concurrent raw charge=%d", charge)
	}
	close(release)
	wg.Wait()
	if m.output != 0 {
		t.Fatal("comment frames leaked reservations")
	}
}

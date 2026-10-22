package main

import (
	"context"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// A reconnect storm on one task (extra readers of an already-watched task)
// must not consume the slots another task needs for its first reattachment.
func TestBackgroundStormKeepsRecoverySlots(t *testing.T) {
	var creates atomic.Int32
	hold := make(chan struct{})
	defer close(hold)
	u := gateUpstream(t, &creates, 2, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		flush()
		<-hold
	})
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	respA, idA := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
	respA.Body.Close()
	respB, idB := openBackgroundTask(t, srv, `{"model":"m","input":"B","background":true,"stream":true}`)
	respB.Body.Close()
	waitReaders := func(id string, want int) {
		t.Helper()
		k.cacheMu.Lock()
		task := k.tasks[id]
		k.cacheMu.Unlock()
		deadline := time.Now().Add(3 * time.Second)
		for {
			task.mu.Lock()
			n := task.readers
			task.mu.Unlock()
			if n == want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("task %s readers=%d want %d", id, n, want)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitReaders(idA, 0)
	waitReaders(idB, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	open := func(id string) int {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/responses/"+id+"?stream=true&starting_after=0", nil)
		req.Header.Set("Authorization", "Bearer owner")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			return 0
		}
		if res.StatusCode != http.StatusOK {
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
			return res.StatusCode
		}
		go func() { <-ctx.Done(); res.Body.Close() }()
		return res.StatusCode
	}
	// Storm: one recovery reader then more readers of task A than the pool holds.
	if status := open(idA); status != http.StatusOK {
		t.Fatalf("first reattachment of A: %d", status)
	}
	var wg sync.WaitGroup
	var accepted, rejected atomic.Int32
	for range maxSockets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch open(idA) {
			case http.StatusOK:
				accepted.Add(1)
			case http.StatusServiceUnavailable:
				rejected.Add(1)
			}
		}()
	}
	wg.Wait()
	if rejected.Load() == 0 {
		t.Fatalf("storm was not bounded: accepted=%d", accepted.Load())
	}
	if status := open(idB); status != http.StatusOK {
		t.Fatalf("storm on A blocked first reattachment of B: %d (storm accepted=%d rejected=%d)", status, accepted.Load(), rejected.Load())
	}
	if creates.Load() != 2 {
		t.Fatalf("hidden generations=%d", creates.Load())
	}
}

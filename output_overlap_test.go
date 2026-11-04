package main

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestOverlappingOutputPressureIsExactAndFittingTurnProgresses(t *testing.T) {
	k := &keeper{work: newWorkingSet()}
	// Sized against the span-path scratch (~2.25n per phase): two 2600KiB
	// frames plus both reservations genuinely exceed the 16MiB output budget.
	aRaw := []byte(`{"type":"response.completed","response":{"id":"up_a","status":"completed","output":[]},"pad":"` + strings.Repeat("x", 2600<<10) + `"}`)
	bRaw := []byte(`{"id":"up_b","status":"completed","output":[],"pad":"` + strings.Repeat("x", 2600<<10) + `"}`)
	ab, bb := responseBuffer{work: k.work}, responseBuffer{work: k.work}
	if err := ab.append(aRaw, maxResponseBytes); err != nil {
		t.Fatal(err)
	}
	if err := bb.append(bRaw, maxResponseBytes); err != nil {
		ab.close()
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var worker sync.WaitGroup
	done := make(chan error, 1)
	collectEventCalls = func() { close(entered); <-release }
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		worker.Wait()
		collectEventCalls = nil
		ab.close()
		bb.close()
	})
	worker.Add(1)
	go func() {
		defer worker.Done()
		var a collector
		_, free, err := k.responseFrame(context.Background(), &a, ab.data, true, true, nil)
		if free != nil {
			free()
		}
		a.release(k.work)
		done <- err
	}()
	<-entered
	k.work.mu.Lock()
	used := k.work.output
	k.work.mu.Unlock()
	needed := responseScratch(len(bb.data))
	if used+needed <= maxCollectedBytes {
		t.Fatal("fixture does not produce real aggregate byte pressure")
	}
	var b collector
	_, free, err := k.responseFrame(context.Background(), &b, bb.data, false, false, nil)
	if free != nil {
		free()
	}
	b.release(k.work)
	k.work.mu.Lock()
	after := k.work.output
	k.work.mu.Unlock()
	if !errors.Is(err, errWorkSize) || after != used {
		t.Fatalf("pressure outcome/charge wrong: used=%d requested=%d limit=%d after=%d err=%v", used, needed, maxCollectedBytes, after, err)
	}
	var small collector
	_, free, err = k.responseFrame(context.Background(), &small, []byte(`{"id":"small","status":"completed","output":[]}`), false, false, nil)
	if err != nil {
		t.Fatal("fitting unrelated response excluded by paused materializer:", err)
	}
	free()
	small.release(k.work)
	once.Do(func() { close(release) })
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	ab.close()
	bb.close()
	if k.work.output != 0 {
		t.Fatal("overlap returned nonzero output credits:", k.work.output)
	}
	t.Logf("bounded refusal: used=%d + requested=%d > limit=%d; fitting small response passed; final credits=0", used, needed, maxCollectedBytes)
}

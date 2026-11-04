package main

import (
	"testing"
	"time"
)

func TestInputWorkingCreditRefusesWithoutWaiting(t *testing.T) {
	m := newWorkingSet()
	release, err := m.reserveInput(maxInputWorkingBytes)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	start := time.Now()
	if free, err := m.reserveInput(1); err == nil {
		free()
		t.Fatal("working-byte capacity exceeded")
	}
	if time.Since(start) > time.Second {
		t.Fatal("byte admission waited instead of refusing genuine pressure")
	}
}
func TestCollectorRefusalBeforeDecode(t *testing.T) {
	m := newWorkingSet()
	m.output = maxCollectedBytes
	var c collector
	if err := c.boundedEvent([]byte(`{"type":"response.output_item.done","output_index":0,"item":{"x":"y"}}`), m); err != errWorkSize {
		t.Fatal(err)
	}
	if len(c.items) != 0 || c.charged != 0 || m.output != maxCollectedBytes {
		t.Fatal("rejected event allocated retained output or stole charge")
	}
}

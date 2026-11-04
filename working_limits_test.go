package main

import (
	"os"
	"path/filepath"
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
func TestStartupTransientCleanup(t *testing.T) {
	dir := t.TempDir()
	k := &keeper{cfg: config{storeDir: dir}, work: newWorkingSet()}
	f, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	stale := f.Name()
	if _, err := f.Write([]byte("private")); err != nil {
		t.Fatal(err)
	}
	// Simulate process death: close FD without normal unlink/account release.
	if err := f.File.Close(); err != nil {
		t.Fatal(err)
	}
	untouched := filepath.Join(dir, "unrelated")
	if err := os.WriteFile(untouched, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := cleanupTransient(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatal("stale file remains")
	}
	if _, err := os.Stat(untouched); err != nil {
		t.Fatal("unrelated file removed")
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

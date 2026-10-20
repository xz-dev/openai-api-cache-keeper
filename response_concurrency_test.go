package main

import (
	"os"
	"path/filepath"
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
func TestSpoolNamespaceDoesNotFollowSymlinks(t *testing.T) {
	for _, where := range []string{"namespace", "owner"} {
		t.Run(where, func(t *testing.T) {
			dir := t.TempDir()
			outside := t.TempDir()
			victim := filepath.Join(outside, "turn-victim")
			os.WriteFile(victim, []byte("foreign"), 0600)
			ns := filepath.Join(dir, spoolDirName)
			if where == "namespace" {
				if err := os.Symlink(outside, ns); err != nil {
					t.Fatal(err)
				}
			} else {
				os.Mkdir(ns, 0700)
				if err := os.Symlink(victim, filepath.Join(ns, "owner")); err != nil {
					t.Fatal(err)
				}
			}
			if err := cleanupTransient(dir); err == nil {
				t.Fatal("unowned symlink namespace accepted")
			}
			if data, err := os.ReadFile(victim); err != nil || string(data) != "foreign" {
				t.Fatal("symlink target touched")
			}
		})
	}
}
func TestSpoolReplacedPathPreservesForeignFileAndCharge(t *testing.T) {
	_, _, k := newTestKeeper(t)
	f, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("held"))
	name := f.Name()
	original := name + "-moved"
	if err := os.Rename(name, original); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(name, []byte("foreign"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err == nil {
		t.Fatal("replaced path cleanup should fail safely")
	}
	if data, err := os.ReadFile(name); err != nil || string(data) != "foreign" {
		t.Fatal("foreign replacement deleted")
	}
	if k.work.disk != f.offset+4 {
		t.Fatalf("unreclaimed file charge=%d", k.work.disk)
	}
}

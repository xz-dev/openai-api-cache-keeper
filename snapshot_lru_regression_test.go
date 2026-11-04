package main

import (
	"context"
	"testing"
	"time"
)

func TestIndependentProductionSnapshotPreservesLRU(t *testing.T) {
	for _, lookup := range []string{"resolveID-control", "snapshotID-production"} {
		t.Run(lookup, func(t *testing.T) {
			s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
			s.maxEntries = 2
			now := time.Unix(100, 0)
			s.now = func() time.Time { return now }
			owner := [32]byte{1}
			if _, err := s.put("A", owner, "", items(`"a"`)); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Second)
			if _, err := s.put("B", owner, "", items(`"b"`)); err != nil {
				t.Fatal(err)
			}
			now = now.Add(time.Second)
			if lookup == "resolveID-control" {
				if _, _, ok := s.resolveID("A", owner, false); !ok {
					t.Fatal("A missing before touch")
				}
			} else {
				h, err := s.snapshotID(context.Background(), "A", owner, false)
				if err != nil {
					t.Fatal(err)
				}
				if err = h.Close(); err != nil {
					t.Fatal(err)
				}
			}
			now = now.Add(time.Second)
			if _, err := s.put("C", owner, "", items(`"c"`)); err != nil {
				t.Fatal(err)
			}
			_, _, a := s.resolveID("A", owner, false)
			_, _, b := s.resolveID("B", owner, false)
			t.Logf("A recently read=%t, B not read=%t", a, b)
			if !a || b {
				t.Error("successful production history read did not update LRU; fresh A evicted instead of stale B")
			}
		})
	}
}

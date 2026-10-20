package main

import (
	"fmt"
	"testing"
	"time"
)

func TestStoreExpirySweepReclaimsEveryAlias(t *testing.T) {
	s := newTestStore(t, time.Minute, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }
	for i := 0; i < 100; i++ {
		mustReviewPut(t, s, fmt.Sprintf("alias-%03d", i), o, "", items(fmt.Sprintf(`"item-%03d"`, i)))
	}
	now = now.Add(2 * time.Minute)
	if err := expireTestStore(s, now); err != nil {
		t.Fatal(err)
	}
	live, meta, _, nodes, aliases := inspectStore(t, s)
	if live != 0 || meta != 0 || nodes != 0 || aliases != 0 {
		t.Fatalf("expiry skipped aliases: live=%d meta=%d nodes=%d aliases=%d", live, meta, nodes, aliases)
	}
}

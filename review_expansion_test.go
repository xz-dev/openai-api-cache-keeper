package main

import (
	"context"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestReviewCoreUniqueExpansionStaysSmall(t *testing.T) {
	_, _, k := newTestKeeper(t, func(c *config) {
		c.maxStoreBytes = 128 << 20
		c.storeMetaBytes = 8 << 20
		c.storePinBytes = 64 << 20
		c.storeCompactBytes = 512 << 20
	})
	owner := [32]byte{12}
	factory := reviewUniqueFactory(768, 64<<10)
	if _, _, err := k.store.accept("unique-seed", owner, factory, "", true, false); err != nil {
		t.Fatal(err)
	}
	runtime.GC()
	var b, a runtime.MemStats
	runtime.ReadMemStats(&b)
	raw, err := k.spoolRequest(context.Background(), strings.NewReader(`{"model":"m","input":[],"previous_response_id":"unique-seed"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	turn, ae := k.prepareDisk(context.Background(), raw, owner, func(id string) ([32]byte, bool) { tip, _, ok := k.store.resolveID(id, owner, false); return tip, ok }, false)
	if ae != nil {
		t.Fatal(ae)
	}
	defer turn.file.Close()
	runtime.ReadMemStats(&a)
	t.Logf("48MiB unique stored history expansion TotalAlloc=%d spans=%d", a.TotalAlloc-b.TotalAlloc, len(turn.spans))
	if a.TotalAlloc-b.TotalAlloc > 8<<20 {
		t.Fatal("expansion exceeded 8MiB alloc")
	}
	if len(turn.spans) != 768 {
		t.Fatal(len(turn.spans))
	}
	next := factory()
	reader := turn.itemReader()
	buf := make([]byte, 0, 64<<10)
	for i := 0; i < 768; i++ {
		want, e := next(buf)
		if e != nil {
			t.Fatal(e)
		}
		got, e := reader(nil)
		if e != nil {
			t.Fatal(e)
		}
		if string(want) != string(got) {
			t.Fatalf("item %d differs", i)
		}
		buf = want
	}
	if _, err := reader(nil); err != io.EOF {
		t.Fatal(err)
	}
	_ = time.Second
}

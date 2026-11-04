package main

import (
	"fmt"
	"strings"
	"testing"
)

// M0 finding: input parsing charged 2x the body, so a body above ~40 MiB
// could never fit the 80 MiB input pool and got 413 "working_set_too_large".
// The scan skeleton is now fixed (design D2): a 48 MiB string input passes
// while the pool holds no other work.
func TestLargeBodyFitsFixedParseCredit(t *testing.T) {
	_, srv, k := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 64 << 20 })
	text := strings.Repeat("x", 48<<20)
	status, body, _ := post(t, srv, "Bearer big", `{"model":"m","stream":true,"store":false,"input":"`+text+`"}`)
	if status != 200 || !strings.Contains(body, "response.completed") {
		t.Fatalf("48 MiB body: status=%d body=%.300s", status, body)
	}
	waitWorking(t, k, 0)
}

// Top-level fields are bounded only by bytes; each new field is charged, so
// a field count beyond the pool is an explicit capacity error, never an
// uncharged map or a 400.
func TestManyTopLevelFieldsCharged(t *testing.T) {
	_, srv, k := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 64 << 20 })
	var b strings.Builder
	b.WriteString(`{"model":"m","input":"x"`)
	for i := 0; i < 400000; i++ {
		fmt.Fprintf(&b, `,"f%d":0`, i)
	}
	b.WriteString(`}`)
	k.work.mu.Lock()
	k.work.input = maxInputWorkingBytes - 16<<20 // other live work holds the pool
	k.work.mu.Unlock()
	status, body, _ := post(t, srv, "Bearer fields", b.String())
	k.work.mu.Lock()
	k.work.input -= maxInputWorkingBytes - 16<<20
	k.work.mu.Unlock()
	if status != 413 && status != 503 {
		t.Fatalf("400k fields over pool: status=%d body=%.300s", status, body)
	}
	if !strings.Contains(body, "working_set") {
		t.Fatalf("refusal does not name the limit: %.300s", body)
	}
	waitWorking(t, k, 0)
}

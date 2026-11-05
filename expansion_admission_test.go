package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// admissionProbe records keeper's live spool bytes each time the gate
// decides (fits runs on admit and on a raising resize).
type admissionProbe struct {
	mu   sync.Mutex
	disk []int64
}

func (p *admissionProbe) reset() []int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	d := p.disk
	p.disk = nil
	return d
}

func expansionKeeper(t *testing.T) (*httptest.Server, *keeper, *admissionProbe) {
	up, srv, k := newWSNativeKeeper(t, func(c *config) {
		c.ttl = time.Hour // a -race run of the 70 MiB turns outlasts the default minute
		c.maxRequestBytes = 140 << 20
		c.maxStoreBytes, c.storePinBytes, c.storeMetaBytes, c.storeCompactBytes = 1<<30, 256<<20, 32<<20, 1<<30
	})
	up.readLimit.Store(-1)
	p := &admissionProbe{}
	k.gate.diskFree = func() (int64, error) { return 1 << 40, nil }
	k.gate.memAvailable = func() (int64, error) {
		k.work.mu.Lock()
		d := k.work.disk
		k.work.mu.Unlock()
		p.mu.Lock()
		p.disk = append(p.disk, d)
		p.mu.Unlock()
		return 901 * mib, nil
	}
	return srv, k, p
}

func turnBody(prev, input string) string {
	body := `{"model":"m","stream":true,"input":"` + input + `"`
	if prev != "" {
		body += `,"previous_response_id":"` + prev + `"`
	}
	return body + "}"
}

// waitSpoolsClosed waits for every turn to end, asserts no spool byte is
// live, and returns and restarts the spool high-water mark.
func waitSpoolsClosed(t *testing.T, k *keeper) int64 {
	t.Helper()
	waitWorking(t, k, 0)
	deadline := time.Now().Add(5 * time.Second)
	for {
		k.work.mu.Lock()
		d, peak := k.work.disk, k.work.diskPeak
		if d == 0 {
			k.work.diskPeak = 0
		}
		k.work.mu.Unlock()
		if d == 0 {
			return peak
		}
		if time.Now().After(deadline) {
			t.Fatalf("spool bytes still live: %d", d)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func wsTerminal(t *testing.T, c *websocket.Conn, frame string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ev map[string]any
		if json.Unmarshal(data, &ev) != nil {
			t.Fatalf("non-JSON frame %.200q", data)
		}
		switch ev["type"] {
		case "response.completed", "response.failed", "error":
			return ev
		}
	}
}

// B2: a small continuation whose stored history expands past 64 MiB is
// admitted before any expansion byte is written, on HTTP and native WS
// (task 1.2, D4 "computes from stored lengths before sending").
func TestExpansionAdmittedBeforeSpool(t *testing.T) {
	srv, k, p := expansionKeeper(t)
	status, out, _ := post(t, srv, "Bearer big", turnBody("", strings.Repeat("x", 70<<20)))
	if status != 200 {
		t.Fatalf("turn1 %d %.200s", status, out)
	}
	id1 := completedID(t, out)
	waitSpoolsClosed(t, k)
	p.reset()

	delta := turnBody(id1, "d")
	status, out, _ = post(t, srv, "Bearer big", delta)
	if status != 200 {
		t.Fatalf("http turn2 %d %.200s", status, out)
	}
	if d := p.reset(); len(d) != 1 || d[0] > int64(len(delta)) {
		t.Fatalf("http: spool bytes at admission %v, want one decision holding only the %d-byte request", d, len(delta))
	}
	waitLedgerEmpty(t, k)
	waitSpoolsClosed(t, k)

	frame := `{"type":"response.create","model":"m","input":"d","previous_response_id":"` + id1 + `"}`
	c := dialWS(t, srv, "Bearer big")
	if ev := wsTerminal(t, c, frame); ev["type"] != "response.completed" {
		t.Fatalf("ws turn2: %v", ev)
	}
	if d := p.reset(); len(d) != 1 || d[0] > int64(len(frame)) {
		t.Fatalf("ws: spool bytes at admission %v, want one decision holding only the %d-byte frame", d, len(frame))
	}
	c.Close(websocket.StatusNormalClosure, "")
	waitLedgerEmpty(t, k)
}

// B2: an expansion over the 256 MiB cap gets the named 413 before any
// expansion byte is written: the only spool bytes are the raw request.
func TestExpansionOverCapRefusedBeforeSpool(t *testing.T) {
	srv, k, _ := expansionKeeper(t)
	text := strings.Repeat("x", 129<<20)
	status, out, _ := post(t, srv, "Bearer big", turnBody("", text))
	if status != 200 {
		t.Fatalf("turn1 %d %.200s", status, out)
	}
	id1 := completedID(t, out)
	waitSpoolsClosed(t, k)
	body := turnBody(id1, text)
	status, out, _ = post(t, srv, "Bearer big", body)
	if status != 413 || !strings.Contains(out, "history_too_large") {
		t.Fatalf("over cap: %d %.300s", status, out)
	}
	if peak := waitSpoolsClosed(t, k); peak > int64(len(body)) {
		t.Fatalf("spool peak %d > raw request %d: expansion bytes written before the cap check", peak, len(body))
	}
	waitLedgerEmpty(t, k)
}

// B1: keeper's spool bytes stay within the gate's k_disk 2 (gate.go): the
// first body spool is reused, never copied, so a first turn peaks at its raw
// request plus its expansion, and a continuation at its expansion plus delta.
func TestSpoolPeakWithinTwoN(t *testing.T) {
	srv, k, _ := expansionKeeper(t)
	body := turnBody("", strings.Repeat("x", 70<<20))
	status, out, _ := post(t, srv, "Bearer big", body)
	if status != 200 {
		t.Fatalf("turn1 %d %.200s", status, out)
	}
	// The expansion wraps the string input in a user item: N is the larger.
	n := int64(len(body)) + 64
	if peak := waitSpoolsClosed(t, k); peak == 0 || peak > 2*n {
		t.Fatalf("first turn spool peak %d, want <= 2N = %d", peak, 2*n)
	}
	id1 := completedID(t, out)
	delta := turnBody(id1, strings.Repeat("y", 1<<20))
	status, out, _ = post(t, srv, "Bearer big", delta)
	if status != 200 {
		t.Fatalf("turn2 %d %.200s", status, out)
	}
	n += int64(len(delta))
	if peak := waitSpoolsClosed(t, k); peak > 2*n {
		t.Fatalf("continuation spool peak %d, want <= 2N = %d", peak, 2*n)
	}
}

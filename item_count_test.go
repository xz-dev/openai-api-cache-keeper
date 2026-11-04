package main

import (
	"strings"
	"testing"
)

// M0 finding: a text body with more than 8192 input items got a misleading
// 400 "input must be a string or an array of items". History and input are
// bounded by bytes and accounting only (design D5), so 11,000 items pass
// and a continuation carries all of them plus the new input.
func TestManyInputItemsAccepted(t *testing.T) {
	const n = 11000
	items := strings.TrimSuffix(strings.Repeat(`{"role":"user","content":"x"},`, n), ",")
	up, srv, _ := newTestKeeper(t, func(c *config) {
		c.maxRequestBytes = 8 << 20
		c.maxStoreBytes = 64 << 20
		c.storeMetaBytes = 64 << 20
		c.storePinBytes = 64 << 20
		c.storeCompactBytes = 256 << 20
	})
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","stream":true,"input":[`+items+`]}`)
	if status != 200 {
		t.Fatalf("%d input items: status=%d body=%.300s", n, status, body)
	}
	id := completedID(t, body)
	if got := len(up.inputs[len(up.inputs)-1]); got != n {
		t.Fatalf("upstream got %d items, want %d", got, n)
	}
	status, body, _ = post(t, srv, "Bearer a", `{"model":"m","stream":true,"previous_response_id":"`+id+`","input":"next"}`)
	if status != 200 {
		t.Fatalf("continuation over %d history items: status=%d body=%.300s", n, status, body)
	}
	if got := len(up.inputs[len(up.inputs)-1]); got <= n+1 {
		t.Fatalf("continuation forwarded %d items, want > %d", got, n+1)
	}
}

func TestManyItemsOverWorkingPoolIsExplicit(t *testing.T) {
	// Span metadata is charged: an item count whose spans exceed the input
	// working pool is a named working-set refusal, never the misleading 400.
	_, srv, _ := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 64 << 20 })
	n := int(maxInputWorkingBytes/16) + 1024
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":[`+strings.TrimSuffix(strings.Repeat("0,", n), ",")+`]}`)
	if status == 400 || strings.Contains(body, "input must be a string") {
		t.Fatalf("misleading refusal: status=%d body=%.300s", status, body)
	}
	if status != 413 && status != 503 {
		t.Fatalf("status=%d body=%.300s; want explicit 413/503", status, body)
	}
}

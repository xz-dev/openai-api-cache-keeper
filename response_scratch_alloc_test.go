package main

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

func allocDuring(fn func()) int64 {
	var a, b runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&a)
	fn()
	runtime.ReadMemStats(&b)
	return int64(b.TotalAlloc - a.TotalAlloc)
}

// The reserved scratch must cover what parse and materialize really allocate,
// measured on adversarial shapes; lowering the bound without lowering real
// allocation fails here.
func TestResponseScratchCoversRealAllocation(t *testing.T) {
	text := func(s string) string {
		return `{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + s + `"}]}`
	}
	bare := func(id, extra, output string) string {
		return `{"id":"` + id + `","object":"response","status":"completed","store":false` + extra + `,"output":[` + output + `]}`
	}
	many := strings.TrimSuffix(strings.Repeat(text(strings.Repeat("y", 200))+",", maxHistoryItems), ",")
	nested := `{"type":"message","content":[` + strings.TrimSuffix(strings.Repeat(`{"type":"output_text","text":"z","annotations":[{"a":[[[{"b":"`+strings.Repeat("q", 2000)+`"}]]]}]},`, 900), ",") + `]}`
	var keys strings.Builder
	for i := range 120 {
		keys.WriteString(`,"k` + strconv.Itoa(i) + strings.Repeat("K", 30<<10) + `":0`)
	}
	shapes := map[string]string{
		"onebig":    bare("r", "", text(strings.Repeat("x", 2<<20))),
		"escapes":   bare("r", "", text(strings.Repeat(`\u00e9\n\"\\`, 2<<20/16))),
		"manyitems": bare("r", "", many),
		"nested":    bare("r", "", nested),
		"dupkeys":   `{"id":"a","store":true,"id":"b","object":"response","status":"completed","output":[],"output":[` + text(strings.Repeat("d", 1<<20)) + `],"store":false,"id":"c"}`,
		"largeid":   bare(strings.Repeat("i", 4<<20), "", text("t")),
		"hugekeys":  bare("r", keys.String(), text("t")),
		// Escaped ids are spliced undecoded; only the kept copy is decoded.
		"escapedid": bare(`\n`+strings.Repeat("i", 3<<20), "", text("t")),
	}
	type frame struct {
		name, raw string
		event     bool
	}
	var frames []frame
	for name, doc := range shapes {
		frames = append(frames, frame{name, doc, false}, frame{name, `{"type":"response.completed","sequence_number":7,"response":` + doc + `}`, true})
	}
	// Unchanged events escape their top-level map (background reuse).
	frames = append(frames, frame{"hugekeys-passthrough", `{"type":"keeper.test"` + keys.String() + `}`, true},
		frame{"escaped-type", `{"type":"\n` + strings.Repeat("t", 900<<10) + `"}`, true},
		frame{"escaped-known-type", `{"type":"respons\u0065.completed","response":` + shapes["escapedid"] + `}`, true},
		frame{"stored-id-passthrough", `{"type":"response.in_progress","response":{"id":"resp_` + strings.Repeat("p", 2<<20) + `","status":"in_progress","store":true}}`, true})
	sawMap := false
	for _, f := range frames {
		{
			name, raw, event := f.name, f.raw, f.event
			if len(raw) > maxResponseBytes {
				t.Fatalf("%s fixture exceeds maxResponseBytes", name)
			}
			for _, stored := range []bool{false, true} {
				n := len(raw)
				bound := responseScratch(n)
				m := newWorkingSet()
				var parse collector
				pb := []byte(raw)
				parsed := allocDuring(func() {
					if err := parse.collect(pb, m, event); err != nil {
						t.Fatal(name, err)
					}
				})
				parse.release(m)
				src := []byte(raw)
				k := &keeper{work: newWorkingSet()}
				var c collector
				var obj map[string]json.RawMessage
				total := allocDuring(func() {
					wire, release, o, err := k.responseFrameParsed(context.Background(), &c, src, event, stored, nil)
					if err != nil {
						t.Fatal(name, err)
					}
					_, obj = wire, o
					release()
				})
				c.release(k.work)
				materialize := total - parsed
				t.Logf("%s event=%v stored=%v n=%d parse=%.2fn materialize=%.2fn total=%.2fn bound=%.2fn", name, event, stored, n,
					float64(parsed)/float64(n), float64(materialize)/float64(n), float64(total)/float64(n), float64(bound)/float64(n))
				if parsed > bound || materialize > bound {
					t.Errorf("%s event=%v stored=%v: parse=%d materialize=%d exceed reserved scratch %d", name, event, stored, parsed, materialize, bound)
				}
				if k.work.output != 0 || m.output != 0 {
					t.Errorf("%s: output credits not balanced: %d/%d", name, k.work.output, m.output)
				}
				if obj != nil {
					sawMap = true
					if live := allocDuring(func() { obj, _ = responseObject(src) }); live > mapCharge(obj) {
						t.Errorf("%s: escaping map allocates %d > charged %d", name, live, mapCharge(obj))
					}
				}
			}
		}
	}
	// Raw invalid UTF-8 in identity tokens is refused before any decode.
	bad := strings.Repeat("\xff", 1<<20)
	for name, f := range map[string]frame{
		"invalid-id":         {"", bare(bad, "", text("t")), false},
		"invalid-event-id":   {"", `{"type":"response.completed","response":` + bare(bad, "", text("t")) + `}`, true},
		"invalid-event-type": {"", `{"type":"` + bad + `","x":1}`, true},
	} {
		n := len(f.raw)
		m := newWorkingSet()
		var c collector
		pb := []byte(f.raw)
		var err error
		parsed := allocDuring(func() { err = c.collect(pb, m, f.event) })
		c.release(m)
		if !errors.Is(err, errUpstreamData) || parsed > responseScratch(n) || parsed > 64<<10 {
			t.Errorf("%s: err=%v parse=%d bound=%d", name, err, parsed, responseScratch(n))
		}
	}
	if !sawMap {
		t.Fatal("no shape exercised the escaping map charge")
	}
}

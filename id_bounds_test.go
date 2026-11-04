package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

// decodeJSONString must agree with encoding/json on every valid UTF-8 token
// (escapes, surrogate pairs, lone surrogates) and refuse raw invalid UTF-8.
func TestDecodeJSONStringMatchesStdlib(t *testing.T) {
	for _, tok := range []string{
		`""`, `"abc"`, `"\n\t\r\b\f\/\\\""`, `"\u00e9\u4e2d"`, `"é中😀"`, `"\ud83d\ude00"`,
		`"\ud83d"`, `"\ud83dx"`, `"\ude00\ud83d"`, `"\ud83d\ud83d\ude00"`, `"\u0000"`, `"\uFFFD�"`,
		`"resp_\u0041"`, `"\u0072\u0065\u0073\u0070\u005fx"`,
	} {
		var want string
		if err := json.Unmarshal([]byte(tok), &want); err != nil {
			t.Fatalf("%s: stdlib: %v", tok, err)
		}
		got, ok := decodeJSONString([]byte(tok))
		if !ok || got != want {
			t.Errorf("%s: got %q ok=%v, want %q", tok, got, ok, want)
		}
		if hasJSONPrefix([]byte(tok), "resp_") != strings.HasPrefix(want, "resp_") {
			t.Errorf("%s: hasJSONPrefix disagrees with decoded %q", tok, want)
		}
	}
	for _, tok := range []string{"\"\xff\"", "\"a\xc3\"", `"\x"`, `"\u12"`, `"a"b"`, "\"\x01\"", `"\`, `x`} {
		if s, ok := decodeJSONString([]byte(tok)); ok {
			t.Errorf("%q decoded to %q, want refusal", tok, s)
		}
	}
}

// B1: an unknown previous_response_id of any size is honored for lookup but
// only a bounded, UTF-8-safe prefix is echoed in the error text.
func TestPreviousNotFoundEchoIsBounded(t *testing.T) {
	_, srv, k := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 8 << 20 })
	n := 1 << 20
	body := `{"input":"x","previous_response_id":"resp_` + strings.Repeat("<", n) + `"}`
	status, resp, _ := post(t, srv, "", body)
	if status != 400 || len(resp) > 1024 {
		t.Fatalf("status=%d response=%d bytes, want 400 with a bounded body", status, len(resp))
	}
	var e struct {
		Error struct{ Code, Param, Message string }
	}
	if err := json.Unmarshal([]byte(resp), &e); err != nil || e.Error.Code != "previous_response_not_found" || e.Error.Param != "previous_response_id" ||
		!strings.HasPrefix(e.Error.Message, "Previous response with id 'resp_<<<") || !strings.HasSuffix(e.Error.Message, "...' not found.") {
		t.Fatalf("error = %+v (%v)", e.Error, err)
	}
	if c := admissionSnapshot(k); c.input != 0 || c.output != 0 {
		t.Fatalf("credits leaked: %+v", c)
	}
	// The cut never splits a rune; short ids are echoed whole.
	id := strings.Repeat("a", 63) + "é" + strings.Repeat("b", 10)
	if m := previousNotFound(id).message; !strings.Contains(m, "'"+strings.Repeat("a", 63)+"...'") {
		t.Fatalf("message %q", m)
	}
	if m := previousNotFound("resp_x").message; m != "Previous response with id 'resp_x' not found." {
		t.Fatalf("message %q", m)
	}
}

// B2/M1/M3/B1: the any-size id decode, the real store lookup's key copy, the
// missing-parent error and (under its own extra credit) the native-parent
// token re-read all fit the credit reserved before them.
func TestPrepareDiskSnapshotPrevIDWithinCredit(t *testing.T) {
	_, _, k := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 16 << 20 })
	lookup := func(id string) (*historySnapshot, error) {
		return k.store.snapshotID(context.Background(), id, [32]byte{}, false)
	}
	for _, tc := range []struct {
		name, id string
		native   bool
	}{
		{"plain", "resp_" + strings.Repeat("p", 4<<20), false},
		{"escaped", `resp_\n` + strings.Repeat("p", 4<<20), false},
		{"invalid-utf8", "resp_" + strings.Repeat("\xff", 1<<20), false},
		{"native-html", strings.Repeat("<", 1<<20), true},
		{"native-escaped", `\u003c` + strings.Repeat("<&", 1<<19), true},
	} {
		body := `{"input":"x","previous_response_id":"` + tc.id + `"}`
		if tc.native {
			body = `{"input":"x","background":true,"previous_response_id":"` + tc.id + `"}`
		}
		input, err := k.spoolRequest(context.Background(), strings.NewReader(body), nil)
		if err != nil {
			t.Fatal(err)
		}
		ctx := context.Background()
		task := &responseTask{}
		if tc.native {
			ctx = context.WithValue(ctx, backgroundTaskKey{}, task)
		}
		credit := inputParseCredit(input) + 2*heapBytes(len(tc.id)+2) + 8<<10 // skeleton + decoded id
		if tc.native {
			credit += heapBytes(len(tc.id) + 2) // the forwarded token's own credit
		}
		var turn *diskTurn
		var apiErr *apiError
		total := allocDuring(func() { turn, apiErr = k.prepareDiskSnapshot(ctx, input, [32]byte{}, lookup, false, false) })
		input.Close()
		if total > credit {
			t.Errorf("%s: allocated %d > parse credit %d", tc.name, total, credit)
		}
		switch {
		case tc.native:
			if apiErr != nil || !task.nativeParent {
				t.Fatalf("%s: native parent not forwarded: %+v", tc.name, apiErr)
			}
			b, _ := io.ReadAll(io.NewSectionReader(turn.file.File, turn.file.offset, turn.file.size))
			turn.file.Close()
			if !bytes.Contains(b, []byte(`"previous_response_id":"`+tc.id+`"`)) {
				t.Errorf("%s: upstream body (%d bytes) does not carry the client token verbatim", tc.name, len(b))
			}
		case tc.name == "invalid-utf8":
			if apiErr == nil || apiErr.status != 400 || apiErr.param != "previous_response_id" {
				t.Errorf("%s: want 400 previous_response_id refusal, got %+v", tc.name, apiErr)
			}
		default:
			if apiErr == nil || apiErr.code != "previous_response_not_found" || len(apiErr.message) > 128 {
				t.Errorf("%s: want bounded previous_response_not_found, got %+v", tc.name, apiErr)
			}
		}
	}
}

// M1/M3: the socket probe's id credit covers an escaped id, and under
// credit pressure it allocates nothing for the id and reports the error.
func TestProbeWSFramePrevIDCredit(t *testing.T) {
	_, _, k := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 8 << 20 })
	n := 1 << 20
	frame := `{"type":"response.create","previous_response_id":"\n` + strings.Repeat("p", n) + `"}`
	sp, err := k.spoolRequest(context.Background(), strings.NewReader(frame), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer sp.Close()
	span := int64(n + 4)
	var p wsProbe
	credit := heapBytes(int(span)) + 8<<10
	if total := allocDuring(func() { p = probeWSFrame(sp) }); total > credit+(64<<10) {
		t.Errorf("probe allocated %d > id credit %d + scanner window", total, credit)
	}
	if held := admissionSnapshot(k).input; held < credit {
		t.Errorf("probe holds %d input credit, want >= %d", held, credit)
	}
	if !p.prevSet || len(p.prev) != n+1 || p.err != nil {
		t.Fatalf("probe prev=%d set=%v err=%v", len(p.prev), p.prevSet, p.err)
	}
	p.close()

	hold, err := k.work.reserveInput(maxInputWorkingBytes - admissionSnapshot(k).input - span)
	if err != nil {
		t.Fatal(err)
	}
	var q wsProbe
	total := allocDuring(func() { q = probeWSFrame(sp) })
	hold()
	if !errors.Is(q.err, errWorkSize) || q.prevSet || !q.isCreate {
		t.Fatalf("pressure probe: err=%v prevSet=%v isCreate=%v", q.err, q.prevSet, q.isCreate)
	}
	if total > 64<<10 {
		t.Errorf("pressure probe allocated %d without credit", total)
	}
	q.close()
	if a := workingError(q.err); a.status != 413 && a.status != 503 {
		t.Fatalf("pressure maps to %+v", a)
	}
}

// B3: identity/status tokens with raw invalid UTF-8 are malformed upstream
// data, refused before any decode allocation; long type/status tokens and
// escaped ids are compared undecoded.
func TestResponseIdentityTokensBounded(t *testing.T) {
	bad := strings.Repeat("\xff", 900<<10)
	for name, raw := range map[string]struct {
		doc   string
		event bool
	}{
		"body-id":      {`{"id":"` + bad + `","object":"response","status":"completed","output":[]}`, false},
		"body-status":  {`{"id":"r","object":"response","status":"` + bad + `","output":[]}`, false},
		"event-type":   {`{"type":"` + bad + `","x":1}`, true},
		"event-id":     {`{"type":"response.completed","response":{"id":"` + bad + `","object":"response","status":"completed","output":[]}}`, true},
		"started-id":   {`{"type":"response.created","response":{"id":"` + bad + `","status":"in_progress"}}`, true},
		"rewritten-id": {`{"type":"response.queued","response":{"id":"` + bad + `","status":"queued"}}`, true},
	} {
		m := newWorkingSet()
		var c collector
		pb := []byte(raw.doc)
		var err error
		total := allocDuring(func() {
			if err = c.collect(pb, m, raw.event); err == nil {
				_, err = rewritePayload(pb, true)
			}
		})
		c.release(m)
		if !errors.Is(err, errUpstreamData) || total > 64<<10 || m.output != 0 {
			t.Errorf("%s: err=%v allocated=%d output=%d", name, err, total, m.output)
		}
	}
	// A short escaped known type still classifies exactly as before.
	var c collector
	m := newWorkingSet()
	if err := c.collect([]byte(`{"type":"respons\u0065.completed","response":{"id":"r","object":"response","status":"completed","output":[]}}`), m, true); err != nil || !c.done || c.id != "r" {
		t.Fatalf("escaped type: err=%v done=%v id=%q", err, c.done, c.id)
	}
	c.release(m)
	if !responseEventFrame([]byte(`{"type":"response.c\u0072eated","response":{"id":"x"}}`)) || responseEventFrame([]byte(`{"type":"response.createdx","response":{}}`)) {
		t.Fatal("event-type token comparison changed")
	}
}

// M2: escaping-map keys above 32 KiB are whole 8 KiB pages; mapCharge must
// still cover them, as well as the worst small size classes.
func TestMapChargeCoversKeyRounding(t *testing.T) {
	for _, klen := range []int{3457, 28673, 30 << 10, 32769, 33000, 40961, 64 << 10} {
		var b strings.Builder
		b.WriteString(`{"type":"keeper.test"`)
		for i := 0; b.Len()+klen < min(1000<<10, 250*klen); i++ { // < 256 members
			k := strconv.Itoa(i)
			b.WriteString(`,"` + k + strings.Repeat("K", klen-len(k)) + `":0`)
		}
		b.WriteByte('}')
		raw := []byte(b.String())
		var obj map[string]json.RawMessage
		live := allocDuring(func() { obj, _ = responseObject(raw) })
		if live > mapCharge(obj) {
			t.Errorf("keylen=%d: escaping map allocates %d > charged %d", klen, live, mapCharge(obj))
		}
	}
}

// M3: the spool-streaming decode must equal decodeJSONString on every token,
// including escapes, surrogate pairs and multibyte runes straddling its
// 4 KiB read buffer, and refuse the same malformed tokens.
func TestDecodeJSONSpanMatchesDecodeJSONString(t *testing.T) {
	_, _, k := newTestKeeper(t)
	pad := func(n int) string { return strings.Repeat("p", n) }
	toks := []string{`""`, `"abc"`, `"\n\t\"\\\/"`, `"\ud83d\ude00"`, `"\ud83d"`, `"é中😀"`, "\"\xff\"", `"\u12"`, `"a"b"`, `"x`, `123`}
	for _, edge := range []int{4090, 4091, 4092, 4093, 4094, 4095, 4096} {
		for _, piece := range []string{`\ud83d\ude00`, `\u00e9`, `\n`, "😀", "中"} {
			toks = append(toks, `"`+pad(edge)+piece+pad(5000)+`"`)
		}
	}
	for _, tok := range toks {
		want, wantOK := decodeJSONString([]byte(tok))
		sp, err := k.spoolRequest(context.Background(), strings.NewReader(tok), nil)
		if err != nil {
			t.Fatal(err)
		}
		got, ok, err := sp.decodeJSONSpan(0, sp.size)
		sp.Close()
		if err != nil || ok != wantOK || got != want {
			t.Errorf("%.40q: got ok=%v err=%v equal=%v, want ok=%v", tok, ok, err, got == want, wantOK)
		}
	}
}

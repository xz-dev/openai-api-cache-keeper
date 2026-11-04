package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"unicode/utf8"
)

var errUpstreamData = errors.New("malformed upstream response")

// responseObject decodes only the top-level keys; values alias raw (cap-limited),
// so the map is valid exactly as long as raw and owns no value bytes.
func responseObject(raw []byte) (map[string]json.RawMessage, error) {
	ms, err := objectMembers(raw)
	if err != nil {
		return nil, err
	}
	obj := make(map[string]json.RawMessage, len(ms))
	for _, m := range ms {
		var key string
		if bytes.IndexByte(m.key, '\\') < 0 {
			key = string(m.key[1 : len(m.key)-1])
		} else {
			var ok bool
			if key, ok = decodeJSONString(m.key); !ok {
				return nil, errUpstreamData
			}
		}
		obj[key] = raw[m.vs:m.ve:m.ve]
	}
	return obj, nil
}

// mapCharge bounds the live memory of a responseObject map whose values alias
// already charged bytes: key strings with allocation rounding plus table slots
// and the member index built with it (~100 + ~100 bytes per member, measured).
func mapCharge(obj map[string]json.RawMessage) int64 {
	if obj == nil {
		return 0
	}
	n := int64(4096 + 256*len(obj))
	for key := range obj {
		n += heapBytes(len(key))
	}
	return n
}

// heapBytes bounds the real allocation of one n-byte object: size classes
// waste < 1/4 (+16 for tiny objects); objects over 32 KiB are whole 8 KiB pages.
func heapBytes(n int) int64 {
	if n > 32<<10 {
		return int64(n+8191) &^ 8191
	}
	return int64(n)*5/4 + 16
}

// arraySpans validates raw as one JSON array and yields its element spans.
func arraySpans(raw []byte, fn func(start, end int) error) error {
	if !json.Valid(raw) {
		return errUpstreamData
	}
	s := sliceScanner(raw)
	s.skipWS()
	if b, _ := s.peek(); b != '[' {
		return errUpstreamData
	}
	err := s.arrayItems(func(start, end int64) error { return fn(int(start), int(end)) })
	if errors.Is(err, errJSONSyntax) || errors.Is(err, io.ErrUnexpectedEOF) {
		return errUpstreamData
	}
	return err
}

// responseItems returns the elements as cap-limited aliases of raw.
func responseItems(raw []byte) ([]json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	var items []json.RawMessage
	err := arraySpans(raw, func(start, end int) error {
		if len(items) >= maxResponseItems {
			return errWorkSize
		}
		items = append(items, raw[start:end:end])
		return nil
	})
	if err != nil {
		return nil, err
	}
	return items, nil
}

// cloneItems is responseItems with ONE independent copy: a counting pass sizes
// the header slice and a single byte buffer exactly; items are subslices whose
// caps sum to the buffer's real capacity.
func cloneItems(raw []byte) ([]json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	count, size := 0, 0
	if err := arraySpans(raw, func(start, end int) error {
		if count >= maxResponseItems {
			return errWorkSize
		}
		count, size = count+1, size+end-start
		return nil
	}); err != nil || count == 0 {
		return nil, err
	}
	items := make([]json.RawMessage, 0, count)
	buf := slices.Grow([]byte(nil), size)
	_ = arraySpans(raw, func(start, end int) error {
		off := len(buf)
		buf = append(buf, raw[start:end]...)
		items = append(items, buf[off:len(buf):len(buf)])
		return nil
	})
	last := len(items) - 1
	items[last] = buf[len(buf)-len(items[last]) : len(buf) : cap(buf)]
	return items, nil
}
func responseString(raw []byte) (string, error) {
	if len(raw) == 0 {
		return "", nil
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		return "", errUpstreamData
	}
	return s, nil
}

// keptID decodes an id the collector keeps: one exact allocation no
// larger than the token (covered by the parse scratch reserved before it);
// raw invalid UTF-8 is malformed upstream data, never expanded.
func keptID(raw []byte) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	id, ok := decodeJSONString(raw)
	if !ok {
		return "", errUpstreamData
	}
	return id, nil
}

// longProtocolName stands in for a type/status token too long to equal any
// protocol name; it is never decoded.
const longProtocolName = "\x00long"

// protocolString decodes a type/status token for comparison. Only tokens
// that could decode to <= 64 bytes are decoded (a bounded allocation); any
// longer string is longProtocolName. Invalid UTF-8 or a non-string fails.
func protocolString(raw []byte) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", nil
	}
	if raw[0] != '"' || !utf8.Valid(raw) {
		return "", errUpstreamData
	}
	if len(raw) > 6*64+2 {
		return longProtocolName, nil
	}
	s, ok := decodeJSONString(raw)
	if !ok {
		return "", errUpstreamData
	}
	return s, nil
}

// smallInt decodes an integer field; a token longer than any int64 cannot
// decode, so it is refused without the stdlib's string copy.
func smallInt(raw []byte, v any) bool {
	return len(raw) <= 20 && json.Unmarshal(raw, v) == nil
}

func (c *collector) response(raw []byte) error {
	ms, err := objectMembers(raw)
	if err != nil {
		return err
	}
	id, err := keptID(memberValue(raw, ms, "id"))
	if err != nil {
		return err
	}
	status, err := protocolString(memberValue(raw, ms, "status"))
	if err != nil {
		return err
	}
	terminal := status == "completed" || status == "incomplete"
	if string(memberValue(raw, ms, "object")) == `"response"` && (status == "" || id == "") {
		return errUpstreamData
	}
	if terminal && id == "" {
		return errUpstreamData
	}
	output, err := cloneItems(memberValue(raw, ms, "output"))
	if err != nil {
		return err
	}
	c.id, c.status, c.done, c.output = id, status, terminal, output
	return nil
}
func (c *collector) event(raw []byte) error {
	ms, err := objectMembers(raw)
	if err != nil {
		return err
	}
	return c.eventFields(raw, ms)
}

// collectEventCalls is a test hook fired once per event decode.
var collectEventCalls func()

func (c *collector) eventFields(raw []byte, ms []jsonMember) error {
	kind, err := protocolString(memberValue(raw, ms, "type"))
	if err != nil || kind == "" {
		return errUpstreamData
	}
	if collectEventCalls != nil {
		collectEventCalls()
	}
	// Track the upstream event sequence so a locally generated terminal
	// event continues it (OpenAI ResponseFailedEvent requires
	// sequence_number).
	var seq int64
	if smallInt(memberValue(raw, ms, "sequence_number"), &seq) && seq >= 0 {
		c.seq, c.seqSet = seq, true
	}
	switch kind {
	case "response.output_item.done":
		var index int
		item := memberValue(raw, ms, "item")
		if !smallInt(memberValue(raw, ms, "output_index"), &index) || index < 0 || index >= 4096 || len(item) == 0 {
			return errUpstreamData
		}
		for len(c.items) <= index {
			c.items = append(c.items, nil)
		}
		c.items[index] = bytes.Clone(item)
	case "response.created", "response.in_progress":
		// Remember the started response so a later local failure can terminate it.
		response := memberValue(raw, ms, "response")
		if rms, err := objectMembers(response); err == nil {
			idRaw := memberValue(response, rms, "id")
			if !utf8.Valid(idRaw) {
				return errUpstreamData
			}
			if id, err := keptID(idRaw); err == nil && id != "" {
				c.started = id
			}
		}
	case "response.completed", "response.incomplete":
		// Validate the event/response pairing before mutating accepted collection.
		response := memberValue(raw, ms, "response")
		rms, err := objectMembers(response)
		if err != nil {
			return err
		}
		status, err := protocolString(memberValue(response, rms, "status"))
		if err != nil || "response."+status != kind {
			return errUpstreamData
		}
		return c.response(response)
	}
	return nil
}

func responseError(err error) *apiError {
	if errors.Is(err, errUpstreamData) || errors.Is(err, io.ErrUnexpectedEOF) {
		return &apiError{status: 502, typ: "upstream_error", message: "upstream response malformed or incomplete"}
	}
	return workingError(err)
}

func (m *workingSet) reserveOutput(n int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.output+n > maxCollectedBytes {
		return false
	}
	m.output += n
	return true
}
func (m *workingSet) freeOutput(n int64) { m.mu.Lock(); m.output -= n; m.mu.Unlock() }
func (c *collector) bytes() int64 {
	n := c.idBytes() + int64((cap(c.items)+cap(c.output))*24)
	for _, v := range c.items {
		n += int64(cap(v))
	}
	for _, v := range c.output {
		n += int64(cap(v))
	}
	return n
}

// idBytes charges the kept identity strings; a suspended collector's state
// lease holds exactly this amount (suspendOutput).
func (c *collector) idBytes() int64 {
	return 512 + heapBytes(len(c.id)) + heapBytes(len(c.started)) + int64(len(c.status))
}

func (c *collector) release(m *workingSet) {
	m.freeOutput(c.charged)
	if c.pendingState != nil {
		c.pendingState.Close()
	}
	*c = collector{}
}

// responseScratch upper-bounds the real allocation of ONE phase over an n-byte
// frame (TestResponseScratchCoversRealAllocation): parse decodes nothing but
// the kept id(s) (one exact copy each, invalid UTF-8 refused) and short
// type/status tokens, so it keeps at most one rounded copy of disjoint value
// spans (<= 5n/4) plus member/item headers; materializing compares type/id
// tokens undecoded and allocates one wire copy (<= n) plus, for an escaping
// event map, its exactly decoded keys (<= 5n/4 with rounding) and slots.
// Reserved BEFORE parse/rewrite, not inferred from output afterward.
func responseScratch(n int) int64 {
	return int64(n)*9/4 + maxResponseItems*24 + 512<<10
}

// decodeScratch is the legacy reservation for paths that still fully decode
// and re-encode a document (background log records, error frames).
func decodeScratch(n int) int64 {
	return 8*int64(n) + 2*maxResponseItems*24 + 256*128
}
func (c *collector) collect(raw []byte, m *workingSet, event bool) error {
	scratch := responseScratch(len(raw))
	// A suspended collector's ids are held by its input-pool state lease, and
	// bytes() counts them again: their output charge is reserved up front and
	// the lease is released only after that transfer succeeds.
	if c.pendingState != nil {
		scratch += c.idBytes()
	}
	if len(raw) > maxResponseBytes || !m.reserveOutput(scratch) {
		return errWorkSize
	}
	var err error
	if event {
		err = c.event(raw)
	} else {
		err = c.response(raw)
	}
	if err != nil {
		m.freeOutput(scratch)
		return err
	}
	actual := c.bytes()
	rest := scratch + c.charged - actual // what the collector did not keep
	if rest < 0 {
		// The scratch bound was violated: keep exactly what is held charged
		// and fail, never charging past the reservation.
		c.charged += scratch
		return errWorkSize
	}
	c.charged = actual
	if c.pendingState != nil {
		c.pendingState.Close()
		c.pendingState = nil
	}
	m.freeOutput(rest)
	if actual > maxResponseBytes {
		return errWorkSize
	}
	return nil
}
func (c *collector) boundedEvent(raw []byte, m *workingSet) error    { return c.collect(raw, m, true) }
func (c *collector) boundedResponse(raw []byte, m *workingSet) error { return c.collect(raw, m, false) }

// Raw frames/bodies use a flat buffer. Capacity and the old+new copy overlap
// are charged before growth; comments cannot allocate one slice per line.
type responseBuffer struct {
	data     []byte
	work     *workingSet
	started  bool // at least one field was appended (empty data is valid)
	lastLine bool // SSE join: the previous data line ended; next byte prepends '\n'
}

func (b *responseBuffer) append(p []byte, limit int) error {
	need := len(b.data) + len(p)
	if need > limit {
		return errWorkSize
	}
	if need > cap(b.data) {
		capacity := min(limit, max(need, max(4096, 2*cap(b.data))))
		if !b.work.reserveOutput(int64(capacity)) {
			return errWorkSize
		}
		next := make([]byte, len(b.data), capacity)
		copy(next, b.data)
		b.work.freeOutput(int64(cap(b.data)))
		b.data = next
	}
	b.data = append(b.data, p...)
	return nil
}
func (b *responseBuffer) close() {
	b.work.freeOutput(int64(cap(b.data)))
	b.data = nil
	b.started = false
	b.lastLine = false
}
func readResponseBody(r io.Reader, m *workingSet) (*responseBuffer, error) {
	b := &responseBuffer{work: m}
	if !m.reserveOutput(32 << 10) {
		return nil, errWorkSize
	}
	defer m.freeOutput(32 << 10)
	buf := make([]byte, 32<<10)
	for {
		n, err := r.Read(buf)
		if e := b.append(buf[:n], maxResponseBytes); e != nil {
			b.close()
			return nil, e
		}
		if err != nil {
			if err == io.EOF {
				return b, nil
			}
			b.close()
			return nil, err
		}
	}
}

// Parsing and rewriting reserve their complete scratch before materializing.
// Return an independently charged wire buffer; byte-pressure admission never waits.
// accept reports whether the turn's history was retained; a terminal response
// that was not retained keeps the upstream store value, so clients never
// continue from an id the keeper does not hold.
func (k *keeper) responseFrame(ctx context.Context, c *collector, raw []byte, event, stored bool, accept func() (bool, error)) ([]byte, func(), error) {
	wire, release, _, err := k.responseFrameParsed(ctx, c, raw, event, stored, accept)
	return wire, release, err
}

// responseFrameParsed collects once; pending terminal frames rematerialize only
// after acceptance. The returned map stays charged until release and describes
// the returned bytes, so a background log can reuse it.
func (k *keeper) responseFrameParsed(ctx context.Context, c *collector, raw []byte, event, stored bool, accept func() (bool, error)) ([]byte, func(), map[string]json.RawMessage, error) {
	var prepare prepareResponseAcceptance
	if accept != nil {
		prepare = func() (*responseAcceptance, error) { return &responseAcceptance{commit: accept}, nil }
	}
	body := &responseBuffer{data: raw, work: k.work}
	raw = nil
	return k.responseFrameProcess(ctx, c, body, false, event, stored, prepare, nil)
}

func (k *keeper) responseFrameProcess(ctx context.Context, c *collector, body *responseBuffer, ownsRaw, event, stored bool, prepare prepareResponseAcceptance, suspend suspendResponseFrame) ([]byte, func(), map[string]json.RawMessage, error) {
	raw := body.data
	if err := ctx.Err(); err != nil {
		return nil, nil, nil, err
	}
	if err := c.collect(raw, k.work, event); err != nil {
		return nil, nil, nil, err
	}
	retained := true
	if prepare != nil {
		acceptance, e := prepare()
		if e != nil {
			return nil, nil, nil, e
		}
		if acceptance != nil && acceptance.commit == nil {
			retained = false
		}
		if acceptance != nil && acceptance.commit != nil {
			if acceptance.close != nil {
				defer acceptance.close()
			}
			pending, e := k.savePendingResponse(raw)
			if e != nil {
				return nil, nil, nil, e
			}
			defer pending.Close()
			if e := c.suspendOutput(k.work); e != nil {
				return nil, nil, nil, e
			}
			raw = nil
			if ownsRaw {
				body.close()
			} else {
				body.data = nil
			}
			var resume func() error
			if suspend != nil {
				resume, e = suspend()
				if e != nil {
					return nil, nil, nil, e
				}
			}
			retained, e = acceptance.commit()
			if e != nil {
				return nil, nil, nil, e
			}
			if e = ctx.Err(); e != nil {
				return nil, nil, nil, e
			}
			if resume != nil {
				if e = resume(); e != nil {
					return nil, nil, nil, e
				}
			}
			restored, e := readResponseBody(pending.spanReader(0, pending.size), k.work)
			if e != nil {
				return nil, nil, nil, e
			}
			defer restored.close()
			raw = restored.data
		}
		stored = stored && retained
	}
	// Materialization (latch scan, splice or copy, escaping map) has its own
	// scratch, reserved before any of it allocates.
	budget := responseScratch(len(raw))
	if !k.work.reserveOutput(budget) {
		return nil, nil, nil, errWorkSize
	}
	// The public id is decided once, at the first response-bearing event:
	// a turn that intends recording advertises the resp_ alias from that
	// point on and never switches back to the upstream id mid-turn, even if
	// the terminal accept later fails (REVIEW3: one response, one id).
	// c.rewrite latches the decision; non-storing turns never latch, so a
	// store:false turn keeps its upstream id as before.
	if prepare != nil && event && stored && responseEventFrame(raw) {
		c.rewrite = true
	}
	// Non-event calls publish a single bare Response object: rewrite exactly
	// when it is a stored (recorded) turn's response.
	rewrite := prepare == nil || c.rewrite
	if !event {
		rewrite = stored
	}
	wire := raw
	var err error
	if rewrite {
		wire, err = rewritePayload(raw, stored)
	}
	if err == nil && len(wire) > maxResponseBytes {
		err = errWorkSize
	}
	if err != nil {
		k.work.freeOutput(budget)
		return nil, nil, nil, err
	}
	// The returned bytes are independent of the raw read buffer and remain
	// charged while the network writer is blocked. A splice is already a
	// fresh exact buffer; unchanged bytes are copied once. An unchanged
	// event's map (values alias owned) stays charged until release.
	owned := wire
	var obj map[string]json.RawMessage
	if sameBytes(wire, raw) {
		owned = append([]byte(nil), raw...)
		if event {
			// No map (the recorder decodes on its own scratch) when a key is
			// not strictly decodable; the bytes still pass through.
			obj, _ = responseObject(owned)
		}
	}
	keep := int64(cap(owned)) + mapCharge(obj)
	if keep > budget {
		if !k.work.reserveOutput(keep - budget) {
			k.work.freeOutput(budget)
			return nil, nil, nil, errWorkSize
		}
	} else {
		k.work.freeOutput(budget - keep)
	}
	release := func() { k.work.freeOutput(keep) }
	return owned, release, obj, nil
}

// sameBytes reports whether a is b itself (rewrite returned its input).
func sameBytes(a, b []byte) bool {
	return len(a) == len(b) && (len(a) == 0 || &a[0] == &b[0])
}
func readDiscard(ctx context.Context, r io.Reader) error {
	// This control-path window is covered by the live socket's state lease.
	_, err := io.CopyBuffer(struct{ io.Writer }{io.Discard}, contextReader{ctx, r}, make([]byte, 1024))
	if err == nil {
		err = ctx.Err()
	}
	return err
}

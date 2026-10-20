package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
)

var errUpstreamData = errors.New("malformed upstream response")

// Decode only bounded object metadata. Raw nested values remain encoded; arrays
// that become retained collections are decoded separately, one item at a time.
func responseObject(raw []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	first, err := d.Token()
	if err != nil || first != json.Delim('{') {
		return nil, errUpstreamData
	}
	obj := make(map[string]json.RawMessage)
	for fields := 0; d.More(); fields++ {
		if fields >= 256 {
			return nil, errWorkSize
		}
		token, err := d.Token()
		if err != nil {
			return nil, errUpstreamData
		}
		key, ok := token.(string)
		if !ok || len(key) > 64<<10 {
			return nil, errWorkSize
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, errUpstreamData
		}
		obj[key] = value
	}
	if _, err := d.Token(); err != nil {
		return nil, errUpstreamData
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errUpstreamData
	}
	return obj, nil
}
func responseItems(raw []byte) ([]json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	first, err := d.Token()
	if err != nil || first != json.Delim('[') {
		return nil, errUpstreamData
	}
	var items []json.RawMessage
	for d.More() {
		if len(items) >= maxHistoryItems {
			return nil, errWorkSize
		}
		var item json.RawMessage
		if d.Decode(&item) != nil {
			return nil, errUpstreamData
		}
		items = append(items, item)
	}
	if _, err := d.Token(); err != nil {
		return nil, errUpstreamData
	}
	if _, err := d.Token(); err != io.EOF {
		return nil, errUpstreamData
	}
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
func (c *collector) response(raw []byte) error {
	obj, err := responseObject(raw)
	if err != nil {
		return err
	}
	id, err := responseString(obj["id"])
	if err != nil {
		return err
	}
	status, err := responseString(obj["status"])
	if err != nil {
		return err
	}
	terminal := status == "completed" || status == "incomplete"
	if string(obj["object"]) == `"response"` && (status == "" || id == "") {
		return errUpstreamData
	}
	if terminal && id == "" {
		return errUpstreamData
	}
	output, err := responseItems(obj["output"])
	if err != nil {
		return err
	}
	c.id, c.status, c.done, c.output = id, status, terminal, output
	return nil
}
func (c *collector) event(raw []byte) error {
	obj, err := responseObject(raw)
	if err != nil {
		return err
	}
	kind, err := responseString(obj["type"])
	if err != nil || kind == "" {
		return errUpstreamData
	}
	switch kind {
	case "response.output_item.done":
		var index int
		if json.Unmarshal(obj["output_index"], &index) != nil || index < 0 || index >= 4096 || len(obj["item"]) == 0 {
			return errUpstreamData
		}
		for len(c.items) <= index {
			c.items = append(c.items, nil)
		}
		c.items[index] = obj["item"]
	case "response.created", "response.in_progress":
		// Remember the started response so a later local failure can terminate it.
		if response, err := responseObject(obj["response"]); err == nil {
			if id, err := responseString(response["id"]); err == nil && id != "" {
				c.started = id
			}
		}
	case "response.completed", "response.incomplete":
		// Validate the event/response pairing before mutating accepted collection.
		response, err := responseObject(obj["response"])
		if err != nil {
			return err
		}
		status, err := responseString(response["status"])
		if err != nil || "response."+status != kind {
			return errUpstreamData
		}
		return c.response(obj["response"])
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
	n := int64(512 + len(c.id) + len(c.started) + len(c.status) + (cap(c.items)+cap(c.output))*24)
	for _, v := range c.items {
		n += int64(cap(v))
	}
	for _, v := range c.output {
		n += int64(cap(v))
	}
	return n
}
func (c *collector) release(m *workingSet) { m.freeOutput(c.charged); c.charged = 0 }
func responseScratch(n int) int64 {
	// Decoder/RawMessage copies, encoder buffer+copy, and bounded field/item
	// metadata. Reserved BEFORE parse/rewrite, not inferred from output afterward.
	return 8*int64(n) + 2*maxHistoryItems*24 + 256*128
}
func (c *collector) collect(raw []byte, m *workingSet, event bool) error {
	scratch := responseScratch(len(raw))
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
	m.freeOutput(scratch + c.charged - actual)
	c.charged = actual
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
	data []byte
	work *workingSet
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
func (b *responseBuffer) close() { b.work.freeOutput(int64(cap(b.data))); b.data = nil }
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

// Parsing and rewriting share the existing SHORT phase with input/commit.
// Return an independently charged wire buffer; no phase spans network I/O.
// accept reports whether the turn's history was retained; a terminal response
// that was not retained keeps the upstream store value, so clients never
// continue from an id the keeper does not hold.
func (k *keeper) responseFrame(ctx context.Context, c *collector, raw []byte, event, stored bool, accept func() (bool, error)) ([]byte, func(), error) {
	leave, err := k.work.enter(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer leave()
	if err := c.collect(raw, k.work, event); err != nil {
		return nil, nil, err
	}
	if accept != nil {
		retained, err := accept()
		if err != nil {
			return nil, nil, err
		}
		stored = stored && retained
	}
	scratch := responseScratch(len(raw))
	if !k.work.reserveOutput(scratch) {
		return nil, nil, errWorkSize
	}
	wire, err := rewritePayload(raw, stored)
	if err != nil {
		k.work.freeOutput(scratch)
		return nil, nil, err
	}
	if len(wire) > maxResponseBytes {
		k.work.freeOutput(scratch)
		return nil, nil, errWorkSize
	}
	// The returned copy is independent of the raw read buffer and remains
	// charged while the network writer is blocked.
	owned := append([]byte(nil), wire...)
	k.work.freeOutput(scratch - int64(cap(owned)))
	release := func() { k.work.freeOutput(int64(cap(owned))) }
	return owned, release, nil
}
func readDiscard(ctx context.Context, r io.Reader) error {
	_, err := io.CopyBuffer(io.Discard, r, make([]byte, 32<<10))
	if err == nil {
		err = ctx.Err()
	}
	return err
}

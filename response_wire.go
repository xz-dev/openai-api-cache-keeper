package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"unicode/utf8"
)

func encodeResponse(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}

// responseEventTypes are the known Responses lifecycle events whose payload
// embeds a full Response object (OpenAI Responses streaming schema). Any
// other response.* name — present or future vendor/namespace extension — is
// relayed byte-for-byte even when it carries a nested response/id field.
var responseEventTypes = map[string]bool{
	"response.created":     true,
	"response.queued":      true,
	"response.in_progress": true,
	"response.completed":   true,
	"response.incomplete":  true,
	"response.failed":      true,
}

// isResponseEventType reports whether the event's type field names a known
// response-bearing lifecycle event worth rewriting.
func isResponseEventType(t string) bool {
	return responseEventTypes[t]
}

// isResponseEventToken is isResponseEventType on the raw type token, compared
// without decoding it (tokenIs).
func isResponseEventToken(tok []byte) bool {
	for name := range responseEventTypes {
		if tokenIs(tok, name) {
			return true
		}
	}
	return false
}

// responseTypeOf extracts the event's type field from a decoded object.
func responseTypeOf(obj map[string]json.RawMessage) string {
	kind, _ := responseString(obj["type"])
	return kind
}

// rewritePayload publishes the client-visible id/store for one Response or
// response-bearing event by splicing value spans: unchanged bytes (key order,
// whitespace, escapes, unknown fields) are copied verbatim into one exactly
// sized buffer. It returns raw itself when nothing changes.
func rewritePayload(raw []byte, stored bool) ([]byte, error) {
	ms, err := objectMembers(raw)
	if err != nil {
		return nil, err
	}
	if r := lastMember(ms, "response"); r >= 0 && isResponseEventToken(memberValue(raw, ms, "type")) {
		nested := raw[ms[r].vs:ms[r].ve]
		edits, err := responseEdits(nested, stored)
		if err != nil || len(edits) == 0 {
			if err != nil {
				return nil, err
			}
			return raw, nil
		}
		for i := range edits {
			edits[i].start += ms[r].vs
			edits[i].end += ms[r].vs
		}
		return splice(raw, append(dropDuplicates(ms, r, "response"), edits...)), nil
	}
	// A bare object is a Response only when it declares object:"response";
	// unrelated events carrying an id must pass through untouched.
	if string(memberValue(raw, ms, "object")) != `"response"` {
		return raw, nil
	}
	edits, err := responseEdits(raw, stored)
	if err != nil || len(edits) == 0 {
		if err != nil {
			return nil, err
		}
		return raw, nil
	}
	return splice(raw, edits), nil
}

// responseEventFrame reports whether raw is a known lifecycle event carrying
// a nested Response (the c.rewrite latch condition).
func responseEventFrame(raw []byte) bool {
	ms, err := objectMembers(raw)
	if err != nil {
		return false
	}
	return lastMember(ms, "response") >= 0 && isResponseEventToken(memberValue(raw, ms, "type"))
}

type spliceEdit struct {
	start, end int
	text       string
}

// splice copies raw with sorted, non-overlapping edits applied.
func splice(raw []byte, edits []spliceEdit) []byte {
	size := len(raw)
	for _, e := range edits {
		size += len(e.text) - (e.end - e.start)
	}
	out := make([]byte, 0, size)
	at := 0
	for _, e := range edits {
		out = append(out, raw[at:e.start]...)
		out = append(out, e.text...)
		at = e.end
	}
	return append(out, raw[at:]...)
}

// dropDuplicates deletes every earlier member named name (keep is the
// last-wins index), so the output carries one value as the old re-encode did.
// A deleted member is always followed by its later duplicate.
func dropDuplicates(ms []jsonMember, keep int, name string) []spliceEdit {
	var edits []spliceEdit
	for i := 0; i < keep; i++ {
		if ms[i].is(name) {
			edits = append(edits, spliceEdit{start: ms[i].ks, end: ms[i+1].ks})
		}
	}
	return edits
}

// responseEdits decides the rewrite of one Response object exactly as the old
// decoded path did: the id is aliased whenever it lacks the resp_ prefix, and
// a stored turn publishes store:true. The id stays undecoded (only its first
// five characters are decoded for the prefix check): the alias is the
// original string token with resp_ inserted after its opening quote. Raw
// invalid UTF-8 in the id is malformed upstream data.
func responseEdits(obj []byte, stored bool) ([]spliceEdit, error) {
	ms, err := objectMembers(obj)
	if err != nil {
		return nil, err
	}
	idAt := lastMember(ms, "id")
	if idAt < 0 {
		return nil, nil
	}
	idRaw := obj[ms[idAt].vs:ms[idAt].ve]
	switch {
	case idRaw[0] != '"':
		if string(idRaw) != "null" { // null is an empty id
			return nil, errUpstreamData
		}
		return nil, nil
	case !utf8.Valid(idRaw):
		return nil, errUpstreamData
	case len(idRaw) == 2:
		return nil, nil
	}
	hasPrefix := hasJSONPrefix(idRaw, "resp_")
	storeAt := lastMember(ms, "store")
	setStore := stored && (storeAt < 0 || string(obj[ms[storeAt].vs:ms[storeAt].ve]) != "true")
	if hasPrefix && !setStore {
		return nil, nil
	}
	var edits []spliceEdit
	for i, m := range ms {
		switch {
		case i == idAt:
			if !hasPrefix {
				edits = append(edits, spliceEdit{start: m.vs + 1, end: m.vs + 1, text: "resp_"})
			}
		case i == storeAt:
			if setStore {
				edits = append(edits, spliceEdit{start: m.vs, end: m.ve, text: "true"})
			}
		case m.is("id") || m.is("store"):
			edits = append(edits, spliceEdit{start: m.ks, end: ms[i+1].ks})
		}
	}
	if setStore && storeAt < 0 {
		end := ms[len(ms)-1].ve
		edits = append(edits, spliceEdit{start: end, end: end, text: `,"store":true`})
	}
	return edits, nil
}

// Compatibility helper for focused rewrite tests; production propagates errors.
func rewriteResponse(raw []byte, stored bool) []byte {
	out, err := rewritePayload(raw, stored)
	if err != nil {
		return raw
	}
	return out
}

// readSSE dispatches complete events: onFrame sees the event's raw bytes
// exactly as read (every byte of the stream lands in exactly one frame,
// including the dispatching blank line) and onData the joined data payload.
//
// WHATWG §9.2.5: CR, LF and CRLF each terminate a line; a lone CR dispatches
// immediately on a live stream — holdCR marks that the next byte, if LF,
// completes that same terminator rather than ending a second line. §9.2.6
// governs BOM strip, comments, and colonless-field handling. A partial event
// at EOF is discarded and reported as truncation, never dispatched.
//
// There is no line buffer: every byte lands in the bounded frame buffer as
// it is read, so an over-limit line is rejected mid-copy instead of being
// accumulated first. data-field identification needs only the first five
// bytes of a line, so lines stay unbounded on disk-side budgets only.
func readSSE(body io.Reader, onFrame func([]byte) error, onData func([]byte) error, budget ...*workingSet) error {
	return readSSEOwned(body, onFrame, func(data *responseBuffer, _ suspendResponseFrame) error { return onData(data.data) }, nil, budget...)
}

func readSSEOwned(body io.Reader, onFrame func([]byte) error, onData func(*responseBuffer, suspendResponseFrame) error, k *keeper, budget ...*workingSet) error {
	m := newWorkingSet()
	if len(budget) > 0 {
		m = budget[0]
	}
	if !m.reserveOutput(32 << 10) {
		return errWorkSize
	}
	scannerHeld := true
	defer func() {
		if scannerHeld {
			m.freeOutput(32 << 10)
		}
	}()
	var pending []*spool
	defer func() {
		for _, f := range pending {
			f.Close()
		}
	}()
	r := bufio.NewReaderSize(body, 32<<10)
	// WHATWG §9.2.6: strip a leading UTF-8 BOM once at the stream head.
	if b, err := r.Peek(3); err == nil && bytes.Equal(b, []byte("\xef\xbb\xbf")) {
		_, _ = r.Discard(3)
	}
	frame := responseBuffer{work: m}
	defer frame.close()
	data := responseBuffer{work: m}
	defer data.close()

	suspend := func() (func() error, error) {
		unread, err := r.Peek(r.Buffered())
		if err != nil {
			return nil, err
		}
		frameLen := int64(len(frame.data))
		saved, err := k.savePendingResponse(frame.data, unread)
		if err != nil {
			return nil, err
		}
		pending = append(pending, saved)
		unread = nil
		frame.close()
		r = nil
		m.freeOutput(32 << 10)
		scannerHeld = false
		return func() error {
			if !m.reserveOutput(32 << 10) {
				return errWorkSize
			}
			scannerHeld = true
			r = bufio.NewReaderSize(io.MultiReader(saved.spanReader(frameLen, saved.size), body), 32<<10)
			restored, err := readResponseBody(saved.spanReader(0, frameLen), m)
			if err != nil {
				return err
			}
			frame = *restored
			return nil
		}, nil
	}

	var lineLen int // content bytes on the current line
	var content int // non-terminator bytes appended since the last dispatch
	var holdCR bool // last byte was CR; a following LF completes its pair
	var prefix int  // -1 = non-data line; 0-4 = matching "data"; 5 = saw ':', next byte may be a stripped space; 6 = value bytes

	// finishLine applies WHATWG §9.2.6 field processing: every data field —
	// "data:v", "data:", or the colonless "data" — appends its value plus
	// one LF to the event buffer. Empty values contribute only the LF. The
	// dispatch step removes exactly the last LF, so empty-data events carry
	// "" as their payload rather than disappearing.
	finishLine := func() error {
		if lineLen == 0 {
			// Blank line: dispatch if the frame holds an event; otherwise
			// the stray blank line is still forwarded in the next frame's
			// raw bytes, keeping the stream byte-faithful.
			if len(frame.data) > 0 {
				if len(data.data) > 0 && data.data[len(data.data)-1] == '\n' {
					data.data = data.data[:len(data.data)-1]
				}
				if data.started && !bytes.Equal(data.data, []byte("[DONE]")) {
					var park suspendResponseFrame
					if k != nil {
						park = suspend
					}
					if err := onData(&data, park); err != nil {
						return err
					}
				}
				if onFrame != nil {
					if err := onFrame(frame.data); err != nil {
						return err
					}
				}
				frame.close()
				data.close()
				content = 0
			}
			return nil
		}
		// A line whose field name is "data" — with or without a colon — is a
		// data field: mark presence, then append the joining LF at line end.
		isData := prefix == 6 || prefix == 5 || (prefix == 4 && lineLen == 4)
		if isData {
			data.started = true
			return data.append([]byte{'\n'}, maxEventBytes)
		}
		return nil
	}

	for {
		b, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				return err
			}
			// §9.2.6: an un-dispatched event at EOF is dropped; the stream is
			// reported as truncated so the caller ends the turn failed. A
			// frame holding only the LF that completed a CRLF after dispatch
			// is not a partial event.
			if content > 0 || lineLen > 0 {
				return io.ErrUnexpectedEOF
			}
			return nil
		}
		if holdCR {
			holdCR = false
			if b == '\n' {
				// LF completes the previous line's CRLF terminator: it is
				// part of the stream bytes, so append to the frame, then
				// continue the new line.
				if err := frame.append([]byte{'\n'}, maxEventBytes); err != nil {
					return err
				}
				continue
			}
			// Lone CR: b begins the next line's content.
		}
		switch b {
		case '\n', '\r':
			if b == '\r' {
				holdCR = true
			}
			if err := frame.append([]byte{b}, maxEventBytes); err != nil {
				return err
			}
			if err := finishLine(); err != nil {
				return err
			}
			lineLen, prefix = 0, 0
		default:
			if err := frame.append([]byte{b}, maxEventBytes); err != nil {
				return err
			}
			content++
			switch prefix {
			case 0:
				if b == 'd' {
					prefix = 1
				} else {
					prefix = -1
				}
			case 1:
				if b == 'a' {
					prefix = 2
				} else {
					prefix = -1
				}
			case 2:
				if b == 't' {
					prefix = 3
				} else {
					prefix = -1
				}
			case 3:
				if b == 'a' {
					prefix = 4
				} else {
					prefix = -1
				}
			case 4:
				if b == ':' {
					prefix = 5
				} else {
					prefix = -1
				}
			case 5:
				// First byte after ':': one optional space is stripped
				// (§9.2.6). prefix 6 marks subsequent bytes as the value.
				if b != ' ' {
					data.started = true
					if err := data.append([]byte{b}, maxEventBytes); err != nil {
						return err
					}
				}
				prefix = 6
			case 6:
				data.started = true
				if err := data.append([]byte{b}, maxEventBytes); err != nil {
					return err
				}
			case -1:
				// Non-data field: frame bytes only.
			}
			lineLen++
		}
	}
}

// Preserve non-data framing/comments, replacing only the data fields.
// frame is the raw event block exactly as read (original terminators,
// including the dispatching blank line); a colonless "data" line is also a
// data field (WHATWG §9.2.6). Rewritten output uses LF terminators.
func writeSSEFrame(w io.Writer, frame, wire []byte) error {
	if wire == nil {
		_, err := w.Write(frame)
		return err
	}
	written := false
	for len(frame) > 0 {
		// Split one raw line: terminator is LF, CR or CRLF.
		line := frame
		var rest []byte
		if i := bytes.IndexAny(frame, "\r\n"); i >= 0 {
			line = frame[:i]
			rest = frame[i+1:]
			if frame[i] == '\r' && len(rest) > 0 && rest[0] == '\n' {
				rest = rest[1:]
			}
		}
		frame = rest
		if len(line) == 0 {
			continue // blank/dispatch line; one is emitted at the end
		}
		if isSSEDataLine(line) {
			if written {
				continue
			}
			written = true
			if _, err := io.WriteString(w, "data: "); err != nil {
				return err
			}
			if _, err := w.Write(wire); err != nil {
				return err
			}
			if _, err := io.WriteString(w, "\n"); err != nil {
				return err
			}
		} else {
			if _, err := w.Write(line); err != nil {
				return err
			}
			if _, err := io.WriteString(w, "\n"); err != nil {
				return err
			}
		}
	}
	_, err := io.WriteString(w, "\n")
	return err
}

// isSSEDataLine reports whether a stripped raw line is a data field: "data:…"
// or the colonless "data" (WHATWG §9.2.6).
func isSSEDataLine(line []byte) bool {
	if i := bytes.IndexByte(line, ':'); i >= 0 {
		line = line[:i]
	}
	return bytes.Equal(line, []byte("data"))
}

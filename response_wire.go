package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
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
func rewritePayload(raw []byte, stored bool) ([]byte, error) {
	obj, err := responseObject(raw)
	if err != nil {
		return nil, err
	}
	return rewriteParsedPayload(obj, raw, stored)
}
// responseEventTypes are the known Responses lifecycle events whose payload
// embeds a full Response object (OpenAI Responses streaming schema). Any
// other response.* name — present or future vendor/namespace extension — is
// relayed byte-for-byte even when it carries a nested response/id field.
var responseEventTypes = map[string]bool{
	"response.created":    true,
	"response.queued":     true,
	"response.in_progress": true,
	"response.completed":  true,
	"response.incomplete": true,
	"response.failed":     true,
}

// isResponseEventType reports whether the event's type field names a known
// response-bearing lifecycle event worth rewriting.
func isResponseEventType(t string) bool {
	return responseEventTypes[t]
}

func rewriteParsedPayload(obj map[string]json.RawMessage, raw []byte, stored bool) ([]byte, error) {
	kind, _ := responseString(obj["type"])
	if nested, ok := obj["response"]; ok && isResponseEventType(kind) {
		fixed, err := rewriteResponseObject(nested, stored)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(fixed, nested) {
			return raw, nil
		}
		obj["response"] = fixed
		return encodeResponse(obj)
	}
	// A bare object is a Response only when it declares object:"response";
	// unrelated events carrying an id must pass through untouched.
	if string(obj["object"]) != `"response"` {
		return raw, nil
	}
	return rewriteParsed(obj, raw, stored)
}
func rewriteResponseObject(raw []byte, stored bool) ([]byte, error) {
	obj, err := responseObject(raw)
	if err != nil {
		return nil, err
	}
	return rewriteParsed(obj, raw, stored)
}

// rewriteParsed reuses an already decoded object so no event is parsed
// twice. The id is rewritten whenever the turn is recorded or still
// undecided (pre-terminal frames cannot know yet); when the caller learned
// retention failed it passes stored=false AND tells rewriteParsedPayload to
// bypass rewriting entirely (see rewriteStored=false in responseFrame).
func rewriteParsed(obj map[string]json.RawMessage, raw []byte, stored bool) ([]byte, error) {
	id, err := responseString(obj["id"])
	if err != nil {
		return nil, err
	}
	if id == "" {
		return raw, nil
	}
	changed := false
	if publicID(id) != id {
		obj["id"], err = encodeResponse(publicID(id))
		if err != nil {
			return nil, err
		}
		changed = true
	}
	if stored && string(obj["store"]) != "true" {
		obj["store"] = json.RawMessage("true")
		changed = true
	}
	if !changed {
		return raw, nil
	}
	return encodeResponse(obj)
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
	m := newWorkingSet()
	if len(budget) > 0 {
		m = budget[0]
	}
	if !m.reserveOutput(32 << 10) {
		return errWorkSize
	}
	defer m.freeOutput(32 << 10)
	r := bufio.NewReaderSize(body, 32<<10)
	// WHATWG §9.2.6: strip a leading UTF-8 BOM once at the stream head.
	if b, err := r.Peek(3); err == nil && bytes.Equal(b, []byte("\xef\xbb\xbf")) {
		_, _ = r.Discard(3)
	}
	frame := responseBuffer{work: m}
	defer frame.close()
	data := responseBuffer{work: m}
	defer data.close()

	var lineLen int  // content bytes on the current line
	var content int  // non-terminator bytes appended since the last dispatch
	var holdCR bool  // last byte was CR; a following LF completes its pair
	var prefix int   // -1 = non-data line; 0-4 = matching "data"; 5 = saw ':', next byte may be a stripped space; 6 = value bytes

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
				payload := data.data
				if len(payload) > 0 && payload[len(payload)-1] == '\n' {
					payload = payload[:len(payload)-1] // strip the trailing LF added per field
				}
				if data.started && !bytes.Equal(payload, []byte("[DONE]")) {
					if err := onData(payload); err != nil {
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

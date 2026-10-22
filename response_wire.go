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
func rewriteParsedPayload(obj map[string]json.RawMessage, raw []byte, stored bool) ([]byte, error) {
	if nested, ok := obj["response"]; ok {
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

// rewriteParsed reuses an already decoded object so no event is parsed twice.
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

// lineReader iterates raw SSE lines. WHATWG §9.2.5 requires CR, LF and CRLF
// to each terminate a line, with a CRLF pair treated as one terminator.
// Encoding is checked once at the stream head (§9.2.6 BOM strip); data bytes
// themselves are never re-encoded.
type lineReader struct {
	r       *bufio.Reader
	buf     []byte // current line without terminator
	rbuf    []byte // current line with its original terminator
	pending []byte // tail after a mid-buffer lone CR
	pendEOF bool   // the pending tail is the stream's last bytes
	err     error
	eof     bool // read error was io.EOF with nothing after it
}

func newLineReader(body io.Reader, m *workingSet) (*lineReader, error) {
	if !m.reserveOutput(32 << 10) {
		return nil, errWorkSize
	}
	r := &lineReader{r: bufio.NewReaderSize(body, 32<<10)}
	// WHATWG §9.2.6: strip a leading UTF-8 BOM once at the stream head.
	if b, err := r.r.Peek(3); err == nil && bytes.Equal(b, []byte("\xef\xbb\xbf")) {
		_, _ = r.r.Discard(3)
	}
	return r, nil
}

// line returns the current line's bytes without their CR/LF terminator;
// raw returns the same line with its original terminator, for byte-faithful
// forwarding of non-data framing.
func (r *lineReader) line() []byte { return r.buf }
func (r *lineReader) raw() []byte  { return r.rbuf }

// scan advances to the next raw line. End of stream is reported by scanErr.
// A line split mid-way by a lone CR keeps the unread tail in pending.
func (r *lineReader) scan() bool {
	for {
		if r.pending != nil {
			// Pending bytes are the remainder of a chunk after a mid-line CR;
			// scan them again for further CRs before treating them as a line.
			part := r.pending
			r.pending = nil
			raw := part
			if i := bytes.IndexByte(part, '\r'); i >= 0 {
				r.buf = part[:i]
				r.rbuf = part[:i+1]
				r.pending = part[i+1:]
				return true
			}
			r.buf = part
			r.rbuf = raw
			if r.pendEOF {
				r.eof = true
			}
			return true
		}
		part, err := r.r.ReadBytes('\n')
		if err != nil && err != bufio.ErrBufferFull {
			r.err = err
		}
		if len(part) == 0 {
			r.eof = err == io.EOF
			return false
		}
		if err == nil {
			// part ends in '\n'; strip the CRLF/LF terminator first so a
			// CR that belongs to the pair is not mistaken for a lone CR.
			body := bytes.TrimSuffix(part, []byte("\n"))
			term := part[len(body):]
			body = bytes.TrimSuffix(body, []byte("\r"))
			// Any remaining mid-line CR is a standalone terminator (§9.2.5).
			if i := bytes.IndexByte(body, '\r'); i >= 0 {
				r.buf = body[:i]
				r.rbuf = part[:i+1]
				r.pending = body[i+1:]
				_ = term
				return true
			}
			r.buf = body
			r.rbuf = part
			return true
		}
		// err != nil: fragment has no '\n'. Any CR in it still ends a line
		// (§9.2.5), including a CR at the very end: no pair can follow
		// because the stream already ended or the buffer is full mid-line.
		if i := bytes.IndexByte(part, '\r'); i >= 0 {
			r.buf = part[:i]
			r.rbuf = part[:i+1]
			r.pending = part[i+1:]
			r.pendEOF = err == io.EOF
			return true
		}
		r.buf = part
		r.rbuf = part
		return true
	}
}

// scanErr reports the terminal read error once scan returns false.
func (r *lineReader) scanErr() error {
	if r.err == io.EOF || r.err == nil {
		return nil
	}
	return r.err
}

func (r *lineReader) close(m *workingSet) { m.freeOutput(32 << 10) }

// readSSE dispatches complete events: onFrame sees the event's raw bytes
// exactly as read (lines with their original terminators, no dispatching
// blank line) and onData the joined data payload. CR/LF/CRLF line endings,
// BOM, comments and non-data fields follow WHATWG §9.2.6. A partial event
// at EOF is discarded and reported as truncation, never dispatched.
func readSSE(body io.Reader, onFrame func([]byte) error, onData func([]byte) error, budget ...*workingSet) error {
	m := newWorkingSet()
	if len(budget) > 0 {
		m = budget[0]
	}
	lr, err := newLineReader(body, m)
	if err != nil {
		return err
	}
	defer lr.close(m)
	frame := responseBuffer{work: m}
	defer frame.close()
	data := responseBuffer{work: m}
	defer data.close()
	dispatch := func() error {
		if len(data.data) > 0 && !bytes.Equal(data.data, []byte("[DONE]")) {
			if err := onData(data.data); err != nil {
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
		return nil
	}
	for lr.scan() {
		line := lr.line()
		if len(line) == 0 && len(lr.raw()) <= 2 {
			// Blank line ends the event; keep its terminator in the raw
			// frame so forwarded bytes match the stream verbatim.
			if len(frame.data) > 0 {
				if err := frame.append(lr.raw(), maxEventBytes); err != nil {
					return err
				}
				if err := dispatch(); err != nil {
					return err
				}
			}
			continue
		}
		if err := frame.append(lr.raw(), maxEventBytes); err != nil {
			return err
		}
		// Field name is the bytes up to ':'; a colonless line is the whole
		// line (§9.2.6). Strip a single leading space from the value.
		name := line
		value := []byte{}
		if i := bytes.IndexByte(line, ':'); i >= 0 {
			name, value = line[:i], line[i+1:]
			if len(value) > 0 && value[0] == ' ' {
				value = value[1:]
			}
		}
		if !bytes.Equal(name, []byte("data")) {
			continue
		}
		if data.started {
			if err := data.append([]byte("\n"), maxEventBytes); err != nil {
				return err
			}
		}
		data.started = true
		if err := data.append(value, maxEventBytes); err != nil {
			return err
		}
	}
	if err := lr.scanErr(); err != nil {
		return err
	}
	// §9.2.6: an un-dispatched event at EOF is dropped; the stream is
	// reported as truncated so the caller ends the turn failed.
	if len(frame.data) > 0 {
		return io.ErrUnexpectedEOF
	}
	return nil
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

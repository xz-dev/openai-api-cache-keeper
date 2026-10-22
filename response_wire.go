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

// One callback per complete raw FRAME, not one allocation/callback per line.
// A delimiter is mandatory. No pending frame is dispatched on read failure.
func readSSE(body io.Reader, onFrame func([]byte) error, onData func([]byte) error, budget ...*workingSet) error {
	m := newWorkingSet()
	if len(budget) > 0 {
		m = budget[0]
	}
	if !m.reserveOutput(32 << 10) {
		return errWorkSize
	}
	defer m.freeOutput(32 << 10)
	reader := bufio.NewReaderSize(body, 32<<10)
	frame := responseBuffer{work: m}
	defer frame.close()
	total, lineStart := 0, 0
	dispatch := func() error {
		var data []byte
		joined := responseBuffer{work: m}
		defer joined.close()
		for lines := frame.data; len(lines) > 0; {
			i := bytes.IndexByte(lines, '\n')
			if i < 0 {
				i = len(lines) - 1
			}
			line := bytes.TrimRight(lines[:i+1], "\r\n")
			lines = lines[i+1:]
			if !bytes.HasPrefix(line, []byte("data:")) {
				continue
			}
			value := bytes.TrimPrefix(line[5:], []byte(" "))
			if data == nil {
				data = value
				continue
			}
			if joined.data == nil {
				if err := joined.append(data, maxEventBytes); err != nil {
					return err
				}
			}
			if err := joined.append([]byte("\n"), maxEventBytes); err != nil {
				return err
			}
			if err := joined.append(value, maxEventBytes); err != nil {
				return err
			}
			data = joined.data
		}
		if len(data) > 0 && !bytes.Equal(data, []byte("[DONE]")) {
			if err := onData(data); err != nil {
				return err
			}
		}
		if onFrame != nil {
			if err := onFrame(frame.data); err != nil {
				return err
			}
		}
		frame.close()
		lineStart = 0
		return nil
	}
	for {
		part, err := reader.ReadSlice('\n')
		total += len(part)
		if total > maxResponseBytes {
			return errWorkSize
		}
		if e := frame.append(part, maxEventBytes); e != nil {
			return e
		}
		if err != nil && err != bufio.ErrBufferFull {
			if err == io.EOF && len(frame.data) == 0 {
				return nil
			}
			if err == io.EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		line := bytes.TrimRight(frame.data[lineStart:], "\r\n")
		lineStart = len(frame.data)
		if len(line) == 0 {
			if err := dispatch(); err != nil {
				return err
			}
		}
	}
}

// Preserve non-data framing/comments, replacing only the data fields. Iteration
// takes slices of the charged raw frame; no per-line slice collection exists.
func writeSSEFrame(w io.Writer, frame, wire []byte) error {
	if wire == nil {
		_, err := w.Write(frame)
		return err
	}
	written := false
	for len(frame) > 0 {
		i := bytes.IndexByte(frame, '\n')
		if i < 0 {
			i = len(frame) - 1
		}
		line := frame[:i+1]
		frame = frame[i+1:]
		if bytes.HasPrefix(line, []byte("data:")) {
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
		}
	}
	return nil
}

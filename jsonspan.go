package main

import (
	"encoding/json"
	"errors"
	"io"
)

// jsonScanner is a byte-level JSON cursor over an io.Reader. It never
// materializes values: scalars and containers are reported as byte spans, so
// a multi-megabyte skipped string costs one 32 KiB sliding window, not a
// decode (streaming requirement: bounded while reading).
type jsonScanner struct {
	r    io.Reader
	buf  []byte // sliding window of not-yet-consumed bytes
	pos  int    // read offset inside buf
	base int64  // absolute offset of buf[0]
	err  error
	eof  bool
}

func newJSONScanner(r io.Reader) *jsonScanner {
	return &jsonScanner{r: r, buf: make([]byte, 0, 32<<10)}
}

// fill ensures at least one byte is available at pos. The window never grows:
// skip routines consume byte-at-a-time, so nothing is retained mid-token.
func (s *jsonScanner) fill() bool {
	for {
		if s.pos < len(s.buf) {
			return true
		}
		if s.eof || s.err != nil {
			return false
		}
		s.base += int64(s.pos)
		s.buf = s.buf[:0]
		s.pos = 0
		n, err := s.r.Read(s.buf[:cap(s.buf)])
		s.buf = s.buf[:n]
		if err != nil {
			if err == io.EOF {
				s.eof = true
			} else {
				s.err = err
			}
		}
	}
}

func (s *jsonScanner) peek() (byte, bool) {
	if !s.fill() {
		return 0, false
	}
	return s.buf[s.pos], true
}

func (s *jsonScanner) next() (byte, bool) {
	if !s.fill() {
		return 0, false
	}
	b := s.buf[s.pos]
	s.pos++
	return b, true
}

// offset is the absolute reader offset just after the last consumed byte.
func (s *jsonScanner) offset() int64 { return s.base + int64(s.pos) }

func (s *jsonScanner) skipWS() {
	for {
		b, ok := s.peek()
		if !ok || (b != ' ' && b != '\t' && b != '\n' && b != '\r') {
			return
		}
		s.pos++
	}
}

var errJSONSyntax = errors.New("invalid JSON")

// skipString consumes a string starting at the current '"'; the cursor ends
// just after the closing quote. Escape sequences keep an escaped quote from
// ending the scan.
func (s *jsonScanner) skipString() error {
	if b, _ := s.peek(); b != '"' {
		return errJSONSyntax
	}
	s.pos++
	for {
		b, ok := s.next()
		if !ok {
			return io.ErrUnexpectedEOF
		}
		switch b {
		case '"':
			return nil
		case '\\':
			if _, ok := s.next(); !ok {
				return io.ErrUnexpectedEOF
			}
		}
	}
}

// skipValue consumes one JSON value and returns its absolute [start,end)
// span. Containers end at their closer; depth tracks nesting iteratively.
func (s *jsonScanner) skipValue() (start, end int64, err error) {
	s.skipWS()
	b, ok := s.peek()
	if !ok {
		return 0, 0, io.ErrUnexpectedEOF
	}
	start = s.offset()
	switch {
	case b == '"':
		if err := s.skipString(); err != nil {
			return 0, 0, err
		}
		return start, s.offset(), nil
	case b == '{' || b == '[':
		s.pos++
		depth := 1
		for depth > 0 {
			c, ok := s.next()
			if !ok {
				return 0, 0, io.ErrUnexpectedEOF
			}
			switch c {
			case '"':
				// Cursor sits after the quote; finish the string manually.
				for {
					d, ok := s.next()
					if !ok {
						return 0, 0, io.ErrUnexpectedEOF
					}
					if d == '\\' {
						if _, ok := s.next(); !ok {
							return 0, 0, io.ErrUnexpectedEOF
						}
					} else if d == '"' {
						break
					}
				}
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
		return start, s.offset(), nil
	default:
		// number / true / false / null: ends at the first structural byte
		// or whitespace.
		for {
			c, ok := s.peek()
			if !ok {
				if s.err != nil {
					return 0, 0, s.err
				}
				return start, s.offset(), nil
			}
			switch c {
			case ',', '}', ']', ' ', '\t', '\n', '\r':
				return start, s.offset(), nil
			}
			s.pos++
		}
	}
}

// objectFields iterates a JSON object's members, yielding each value's span.
// Keys are read back through fn's own bounded read (64 KiB cap); large or
// absent keys surface as empty strings.
func (s *jsonScanner) objectFields(readSpan func(start, end int64) []byte, fn func(key string, start, end int64) error) error {
	s.skipWS()
	if b, _ := s.peek(); b != '{' {
		return errJSONSyntax
	}
	s.pos++
	s.skipWS()
	if b, _ := s.peek(); b == '}' {
		s.pos++
		return nil
	}
	for fields := 0; ; fields++ {
		if fields >= 256 {
			return errWorkSize
		}
		s.skipWS()
		ks := s.offset()
		if err := s.skipString(); err != nil {
			return err
		}
		ke := s.offset()
		s.skipWS()
		if b, ok := s.next(); !ok || b != ':' {
			return errJSONSyntax
		}
		vs, ve, err := s.skipValue()
		if err != nil {
			return err
		}
		var key string
		if raw := readSpan(ks, ke); raw != nil {
			key, _ = readJSONString(raw)
		}
		if err := fn(key, vs, ve); err != nil {
			return err
		}
		s.skipWS()
		c, ok := s.next()
		if !ok {
			return io.ErrUnexpectedEOF
		}
		if c == '}' {
			return nil
		}
		if c != ',' {
			return errJSONSyntax
		}
	}
}

// arrayItems iterates a JSON array's elements, yielding each element's span.
func (s *jsonScanner) arrayItems(fn func(start, end int64) error) error {
	s.skipWS()
	if b, _ := s.peek(); b != '[' {
		return errJSONSyntax
	}
	s.pos++
	s.skipWS()
	if b, _ := s.peek(); b == ']' {
		s.pos++
		return nil
	}
	for {
		vs, ve, err := s.skipValue()
		if err != nil {
			return err
		}
		if err := fn(vs, ve); err != nil {
			return err
		}
		s.skipWS()
		c, ok := s.next()
		if !ok {
			return io.ErrUnexpectedEOF
		}
		if c == ']' {
			return nil
		}
		if c != ',' {
			return errJSONSyntax
		}
	}
}

func readJSONString(raw []byte) (string, bool) {
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", false
	}
	return s, true
}

// spoolScanner adapts the scanner onto a spool; spans are spool-relative
// offsets usable with ReadAt.
func spoolScanner(f *spool) *jsonScanner {
	return newJSONScanner(io.NewSectionReader(f.File, f.offset, f.size))
}

// keySlice re-reads a bounded span (keys/ids); oversized spans return nil.
func (f *spool) keySlice(start, end int64) []byte {
	if n := end - start; n < 0 || n > 64<<10 {
		return nil
	}
	b := make([]byte, end-start)
	if _, err := f.ReadAt(b, start); err != nil {
		return nil
	}
	return b
}

// spanReader returns a bounded reader over one span for streaming copies.
func (f *spool) spanReader(start, end int64) *io.SectionReader {
	return io.NewSectionReader(f.File, f.offset+start, end-start)
}

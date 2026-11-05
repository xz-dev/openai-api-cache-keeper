package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"unicode/utf16"
	"unicode/utf8"
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
	// keyStart is the offset of the current member's key while objectFields
	// calls its fn.
	keyStart int64
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
	return s.objectFieldsUpTo(256, readSpan, fn)
}

// allObjectFields is objectFields with no member cap, for a scan that keeps
// nothing per member (stripping the ticket member must see every member).
func (s *jsonScanner) allObjectFields(readSpan func(start, end int64) []byte, fn func(key string, start, end int64) error) error {
	return s.objectFieldsUpTo(-1, readSpan, fn)
}

// objectFieldsUpTo fails with errWorkSize past maxFields members; < 0: no cap.
func (s *jsonScanner) objectFieldsUpTo(maxFields int, readSpan func(start, end int64) []byte, fn func(key string, start, end int64) error) error {
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
		if maxFields >= 0 && fields >= maxFields {
			return errWorkSize
		}
		s.skipWS()
		ks := s.offset()
		s.keyStart = ks
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

// sliceScanner scans in-memory bytes in place: spans index raw directly and
// nothing is copied through a window.
func sliceScanner(raw []byte) *jsonScanner { return &jsonScanner{buf: raw, eof: true} }

// jsonMember is one object member as spans of the scanned bytes: key is the
// raw key token (quotes included), ks its start, [vs,ve) the value.
type jsonMember struct {
	key        []byte
	ks, vs, ve int
}

// is reports whether the decoded key equals an ASCII name without allocating
// for unescaped keys; an escaped ASCII char takes at most 6 raw bytes.
func (m jsonMember) is(name string) bool { return tokenIs(m.key, name) }

// tokenIs reports whether a raw JSON value token is the string name (ASCII)
// without decoding: only an escaped token short enough to match is decoded.
func tokenIs(tok []byte, name string) bool {
	if len(tok) < 2 || tok[0] != '"' {
		return false
	}
	k := tok[1 : len(tok)-1]
	if bytes.IndexByte(k, '\\') < 0 {
		return string(k) == name
	}
	if len(k) > 6*len(name) {
		return false
	}
	s, ok := readJSONString(tok)
	return ok && s == name
}

// jsonChar decodes the first character of string-token content s (after the
// opening quote) exactly as encoding/json does, except that raw invalid UTF-8
// fails instead of becoming U+FFFD. size is the raw bytes consumed.
func jsonChar(s []byte) (r rune, size int, ok bool) {
	if len(s) == 0 || s[0] == '"' || s[0] < 0x20 {
		return 0, 0, false
	}
	if s[0] != '\\' {
		r, size = utf8.DecodeRune(s)
		return r, size, r != utf8.RuneError || size == 3
	}
	if len(s) < 2 {
		return 0, 0, false
	}
	switch s[1] {
	case '"', '\\', '/':
		return rune(s[1]), 2, true
	case 'b':
		return '\b', 2, true
	case 'f':
		return '\f', 2, true
	case 'n':
		return '\n', 2, true
	case 'r':
		return '\r', 2, true
	case 't':
		return '\t', 2, true
	case 'u':
		r, ok = hex4(s[2:])
		if !ok {
			return 0, 0, false
		}
		if !utf16.IsSurrogate(r) {
			return r, 6, true
		}
		if len(s) >= 12 && s[6] == '\\' && s[7] == 'u' {
			if r2, ok := hex4(s[8:]); ok {
				if d := utf16.DecodeRune(r, r2); d != utf8.RuneError {
					return d, 12, true
				}
			}
		}
		return utf8.RuneError, 6, true
	}
	return 0, 0, false
}

func hex4(s []byte) (rune, bool) {
	if len(s) < 4 {
		return 0, false
	}
	var r rune
	for _, c := range s[:4] {
		switch {
		case '0' <= c && c <= '9':
			c -= '0'
		case 'a' <= c && c <= 'f':
			c -= 'a' - 10
		case 'A' <= c && c <= 'F':
			c -= 'A' - 10
		default:
			return 0, false
		}
		r = r<<4 | rune(c)
	}
	return r, true
}

// decodeJSONString decodes one string token into ONE exactly sized
// allocation (a counting pass first); the decoded form is never longer than
// the token. Raw invalid UTF-8 fails (the stdlib would triple it into U+FFFD
// with buffer doubling), so copy+decode of a token costs at most 2*len(tok).
func decodeJSONString(tok []byte) (string, bool) {
	if len(tok) < 2 || tok[0] != '"' || tok[len(tok)-1] != '"' {
		return "", false
	}
	s := tok[1 : len(tok)-1]
	n := 0
	for i := 0; i < len(s); {
		r, size, ok := jsonChar(s[i:])
		if !ok {
			return "", false
		}
		n += utf8.RuneLen(r)
		i += size
	}
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < len(s); {
		r, size, _ := jsonChar(s[i:])
		if size == 1 {
			b.WriteByte(s[i])
		} else {
			b.WriteRune(r)
		}
		i += size
	}
	return b.String(), true
}

// decodeJSONSpan is decodeJSONString over a spooled string token without a
// whole raw copy: the only allocation is the result, at most the token's
// length. ok=false is a malformed token; err is a read failure.
func (f *spool) decodeJSONSpan(start, end int64) (string, bool, error) {
	if end-start < 2 {
		return "", false, nil
	}
	br := bufio.NewReaderSize(f.spanReader(start, end), 4096)
	if c, err := br.ReadByte(); err != nil || c != '"' {
		return "", false, err
	}
	var b strings.Builder
	b.Grow(int(end - start - 2)) // decoded form is never longer
	for {
		p, err := br.Peek(12)
		if err != nil && err != io.EOF {
			return "", false, err
		}
		if len(p) == 0 {
			return "", false, nil
		}
		if p[0] == '"' {
			if len(p) != 1 { // bytes after the closing quote
				return "", false, nil
			}
			return b.String(), true, nil
		}
		q, _ := br.Peek(br.Buffered())
		j := 0
		for j < len(q) && q[j] >= 0x20 && q[j] < utf8.RuneSelf && q[j] != '"' && q[j] != '\\' {
			j++
		}
		if j > 0 {
			b.Write(q[:j])
			_, _ = br.Discard(j)
			continue
		}
		r, size, ok := jsonChar(p)
		if !ok {
			return "", false, nil
		}
		b.WriteRune(r)
		_, _ = br.Discard(size)
	}
}

// hasJSONPrefix reports whether string token tok decodes to text starting
// with the ASCII prefix, decoding at most len(prefix) characters.
func hasJSONPrefix(tok []byte, prefix string) bool {
	if len(tok) < 2 || tok[0] != '"' {
		return false
	}
	s := tok[1 : len(tok)-1]
	for i := 0; i < len(prefix); i++ {
		r, size, ok := jsonChar(s)
		if !ok || r != rune(prefix[i]) {
			return false
		}
		s = s[size:]
	}
	return true
}

// objectMembers validates raw as one JSON object and returns its top-level
// members in order, with the same field/key bounds as the old decoder path.
func objectMembers(raw []byte) ([]jsonMember, error) {
	if !json.Valid(raw) {
		return nil, errUpstreamData
	}
	s := sliceScanner(raw)
	s.skipWS()
	if b, _ := s.peek(); b != '{' {
		return nil, errUpstreamData
	}
	s.pos++
	s.skipWS()
	var ms []jsonMember
	if b, _ := s.peek(); b == '}' {
		return ms, nil
	}
	for {
		if len(ms) >= 256 {
			return nil, errWorkSize
		}
		s.skipWS()
		ks := s.pos
		if s.skipString() != nil {
			return nil, errUpstreamData
		}
		ke := s.pos
		if ke-ks-2 > 64<<10 {
			return nil, errWorkSize
		}
		s.skipWS()
		s.pos++ // ':' (json.Valid)
		vs, ve, err := s.skipValue()
		if err != nil {
			return nil, errUpstreamData
		}
		ms = append(ms, jsonMember{key: raw[ks:ke:ke], ks: ks, vs: int(vs), ve: int(ve)})
		s.skipWS()
		if c, _ := s.next(); c == '}' {
			return ms, nil
		}
	}
}

// lastMember is the member a last-wins decoder would keep; -1 when absent.
func lastMember(ms []jsonMember, name string) int {
	for i := len(ms) - 1; i >= 0; i-- {
		if ms[i].is(name) {
			return i
		}
	}
	return -1
}

// memberValue aliases the last-wins value of name in raw (cap-limited), or nil.
func memberValue(raw []byte, ms []jsonMember, name string) []byte {
	if i := lastMember(ms, name); i >= 0 {
		return raw[ms[i].vs:ms[i].ve:ms[i].ve]
	}
	return nil
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
	return newJSONScanner(f.section(0, f.size))
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

// spanBytes reads one whole value span of any size; the caller owns its credit.
func (f *spool) spanBytes(start, end int64) ([]byte, error) {
	if start < 0 || end < start || end > f.size {
		return nil, errJSONSyntax
	}
	b := make([]byte, end-start)
	if _, err := f.ReadAt(b, start); err != nil {
		return nil, err
	}
	return b, nil
}

// spanReader returns a bounded reader over one span for streaming copies.
func (f *spool) spanReader(start, end int64) io.Reader {
	return f.section(start, end-start)
}

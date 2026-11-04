package main

import (
	"bufio"
	"context"
	"io"
)

// jsonValidateCredit is the constant heap of validJSONStream: the read
// buffer plus the nesting bitset. Validation no longer maps the spool, so its
// charge does not grow with the body (design D2: skeleton, not body).
const jsonValidateCredit int64 = 64<<10 + maxJSONDepth/8 + 1024

// maxJSONDepth matches encoding/json's nesting limit, so this validator
// accepts exactly what json.Valid accepted.
const maxJSONDepth = 10000

// validJSONStream reports whether r holds exactly one RFC 8259 JSON value
// (with optional surrounding whitespace), with the same acceptance as
// json.Valid, in constant memory. ctx is checked between buffer refills.
func validJSONStream(ctx context.Context, r io.Reader) error {
	v := &jsonValidator{br: bufio.NewReaderSize(contextReader{ctx, r}, 32<<10)}
	err := v.value()
	if err == nil {
		v.ws()
		if _, e := v.br.ReadByte(); e != io.EOF {
			err = errJSONSyntax
			if e != nil {
				v.err = e
			}
		}
	}
	if v.err != nil { // a read failure or cancellation, not malformed JSON
		return v.err
	}
	if err != nil {
		return err
	}
	return ctx.Err()
}

type jsonValidator struct {
	br    *bufio.Reader
	err   error // first non-EOF read error
	depth int
	stack [maxJSONDepth/8 + 1]byte // 1 = object, 0 = array
}

func (v *jsonValidator) next() (byte, bool) {
	b, err := v.br.ReadByte()
	if err != nil && err != io.EOF && v.err == nil {
		v.err = err
	}
	return b, err == nil
}

func (v *jsonValidator) ws() {
	for {
		b, err := v.br.ReadByte()
		if err != nil {
			return
		}
		if b != ' ' && b != '\t' && b != '\n' && b != '\r' {
			_ = v.br.UnreadByte()
			return
		}
	}
}

func (v *jsonValidator) push(object bool) bool {
	if v.depth >= maxJSONDepth {
		return false
	}
	if object {
		v.stack[v.depth/8] |= 1 << (v.depth % 8)
	} else {
		v.stack[v.depth/8] &^= 1 << (v.depth % 8)
	}
	v.depth++
	return true
}

func (v *jsonValidator) object() bool {
	d := v.depth - 1
	return v.stack[d/8]&(1<<(d%8)) != 0
}

// value validates one value iteratively; containers use the bitset stack.
func (v *jsonValidator) value() error {
	for {
		v.ws()
		b, ok := v.next()
		if !ok {
			return errJSONSyntax
		}
		switch {
		case b == '{' || b == '[':
			if !v.push(b == '{') {
				return errJSONSyntax
			}
			v.ws()
			c, ok := v.next()
			if !ok {
				return errJSONSyntax
			}
			if (b == '{' && c == '}') || (b == '[' && c == ']') {
				v.depth--
				break
			}
			_ = v.br.UnreadByte()
			if b == '{' {
				if err := v.key(); err != nil {
					return err
				}
			}
			continue // the first member's value
		case b == '"':
			if err := v.str(); err != nil {
				return err
			}
		case b == '-' || (b >= '0' && b <= '9'):
			if err := v.number(b); err != nil {
				return err
			}
		case b == 't':
			if !v.lit("rue") {
				return errJSONSyntax
			}
		case b == 'f':
			if !v.lit("alse") {
				return errJSONSyntax
			}
		case b == 'n':
			if !v.lit("ull") {
				return errJSONSyntax
			}
		default:
			return errJSONSyntax
		}
		// After a complete value: close containers or move to the next member.
		for {
			if v.depth == 0 {
				return nil
			}
			v.ws()
			c, ok := v.next()
			if !ok {
				return errJSONSyntax
			}
			if c == ',' {
				if v.object() {
					if err := v.key(); err != nil {
						return err
					}
				}
				break
			}
			if (v.object() && c == '}') || (!v.object() && c == ']') {
				v.depth--
				continue
			}
			return errJSONSyntax
		}
	}
}

// key validates `"name" :` before an object member's value.
func (v *jsonValidator) key() error {
	v.ws()
	if b, ok := v.next(); !ok || b != '"' {
		return errJSONSyntax
	}
	if err := v.str(); err != nil {
		return err
	}
	v.ws()
	if b, ok := v.next(); !ok || b != ':' {
		return errJSONSyntax
	}
	return nil
}

// str validates a string body after its opening quote.
func (v *jsonValidator) str() error {
	for {
		b, ok := v.next()
		if !ok {
			return errJSONSyntax
		}
		switch {
		case b == '"':
			return nil
		case b < 0x20:
			return errJSONSyntax
		case b == '\\':
			e, ok := v.next()
			if !ok {
				return errJSONSyntax
			}
			switch e {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
			case 'u':
				for i := 0; i < 4; i++ {
					h, ok := v.next()
					if !ok || !isHex(h) {
						return errJSONSyntax
					}
				}
			default:
				return errJSONSyntax
			}
		}
	}
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

// number validates -?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)? whose
// first byte is b; the byte after it is left unread.
func (v *jsonValidator) number(b byte) error {
	if b == '-' {
		var ok bool
		if b, ok = v.next(); !ok || b < '0' || b > '9' {
			return errJSONSyntax
		}
	}
	if b != '0' {
		v.digits()
	}
	c, ok := v.next()
	if ok && c == '.' {
		if v.digits() == 0 {
			return errJSONSyntax
		}
		c, ok = v.next()
	}
	if ok && (c == 'e' || c == 'E') {
		c, ok = v.next()
		if ok && (c == '+' || c == '-') {
			_, _ = c, ok
		} else if ok {
			_ = v.br.UnreadByte()
		}
		if v.digits() == 0 {
			return errJSONSyntax
		}
		return nil
	}
	if ok {
		_ = v.br.UnreadByte()
	}
	return nil
}

func (v *jsonValidator) digits() int {
	n := 0
	for {
		b, ok := v.next()
		if !ok {
			return n
		}
		if b < '0' || b > '9' {
			_ = v.br.UnreadByte()
			return n
		}
		n++
	}
}

func (v *jsonValidator) lit(rest string) bool {
	for i := 0; i < len(rest); i++ {
		if b, ok := v.next(); !ok || b != rest[i] {
			return false
		}
	}
	return true
}

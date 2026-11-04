package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/coder/websocket"
)

// Known protocol buffers and initial pump stacks, not a connection-count cap.
// Parser stack/buffer growth uses the separate actual-size input/output credits.
const protocolBufferBytes int64 = 4096
const initialStackBytes int64 = 2048

func retainedHeaderBytes(h http.Header) int64 {
	var payload, values int64
	for key, list := range h {
		payload += int64(len(key))
		values += int64(cap(list))
		for _, value := range list {
			payload += int64(len(value))
		}
	}
	// Retained strings plus up to four copied maps/value-slice sets across
	// hop cleanup, transport request, local reply and retained task headers.
	return payload + 4*(int64(len(h))*128+values*16)
}

func turnStateBytes(h http.Header) int64 {
	return int64(unsafe.Sizeof(http.Request{})+unsafe.Sizeof(http.Response{})+unsafe.Sizeof(diskTurn{})+unsafe.Sizeof(collector{})) + 2*(protocolBufferBytes+initialStackBytes) + 256 + retainedHeaderBytes(h)
}
func socketStateBytes(h http.Header) int64 {
	return 2*int64(unsafe.Sizeof(websocket.Conn{})) + int64(unsafe.Sizeof(wsNativeInflight{})+unsafe.Sizeof(wsSession{})) + 4*protocolBufferBytes + 3*initialStackBytes + 1024 + retainedHeaderBytes(h)
}
func taskStateBytes(h http.Header) int64 {
	return int64(unsafe.Sizeof(responseTask{})+unsafe.Sizeof(http.Request{})) + 2*protocolBufferBytes + 2*initialStackBytes + 1024 + retainedHeaderBytes(h)
}
func readerStateBytes(h http.Header) int64 {
	// Persistent protocol metadata only; follow reserves its send-copy window
	// independently from the existing output pool before allocation.
	return int64(unsafe.Sizeof(spoolBody{})+unsafe.Sizeof(http.Request{})) + 2*protocolBufferBytes + initialStackBytes + 512 + retainedHeaderBytes(h)
}
func spoolStateBytes(dir string) int64 {
	// Regular-file descriptor/object metadata includes one allocator/slab page;
	// cgroup kernel/page-cache use remains part of the independent runtime gate.
	return int64(unsafe.Sizeof(spool{})+unsafe.Sizeof(os.File{})) + 4096 + 2*int64(len(dir)) + 256
}

type stateLease struct {
	mu      sync.Mutex
	refs    int
	owner   sync.Once
	free    func()
	counter *atomic.Int64
	extras  []func()
}

func (m *workingSet) state(n int64, counter *atomic.Int64) (*stateLease, error) {
	free, err := m.reserveInput(n)
	if err != nil {
		return nil, err
	}
	if counter != nil {
		counter.Add(1)
	}
	return &stateLease{refs: 1, free: free, counter: counter}, nil
}
func (s *stateLease) drop() {
	s.mu.Lock()
	s.refs--
	last := s.refs == 0
	extras := s.extras
	if last {
		s.extras = nil
	}
	s.mu.Unlock()
	if last {
		for _, free := range extras {
			free()
		}
		if s.counter != nil {
			s.counter.Add(-1)
		}
		s.free()
	}
}
func (s *stateLease) extend(m *workingSet, n int64) error {
	free, err := m.reserveInput(n)
	if err != nil {
		return err
	}
	s.mu.Lock()
	if s.refs == 0 {
		s.mu.Unlock()
		free()
		return io.ErrClosedPipe
	}
	s.extras = append(s.extras, free)
	s.mu.Unlock()
	return nil
}

func (s *stateLease) Close() { s.owner.Do(s.drop) }
func (s *stateLease) retain() func() {
	s.mu.Lock()
	s.refs++
	s.mu.Unlock()
	var once sync.Once
	return func() { once.Do(s.drop) }
}

type requestStateKey struct{}
type stateBody struct {
	io.ReadCloser
	free func()
	once sync.Once
	err  error
}

func (b *stateBody) Close() error {
	b.once.Do(func() { b.err = b.ReadCloser.Close(); b.free() })
	return b.err
}
func retainRequestState(ctx context.Context, body io.Reader) io.Reader {
	lease, _ := ctx.Value(requestStateKey{}).(*stateLease)
	closer, ok := body.(io.ReadCloser)
	if lease == nil || !ok {
		return body
	}
	return &stateBody{ReadCloser: closer, free: lease.retain()}
}

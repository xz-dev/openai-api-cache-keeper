package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Input mappings, scanner/copy windows and retained span metadata share this
// actual-byte pool. Output's existing independent 16-MiB try-reserve is unchanged.
const maxInputWorkingBytes int64 = 80 << 20
const requestCopyCredit int64 = 96 << 10    // 32-KiB copy plus UTF-8 tail/growth overlap
const parseMetadataCredit int64 = 512 << 10 // scanner/keys/maps and bounded item-span overlap

func (m *workingSet) reserveInput(n int64) (func(), error) {
	if !m.tryInput(n) {
		return nil, errWorkSize
	}
	var once sync.Once
	return func() { once.Do(func() { m.freeInput(n) }) }, nil
}

func (m *workingSet) tryInput(n int64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if n < 0 || n > maxInputWorkingBytes-m.input {
		return false
	}
	m.input += n
	return true
}

func (m *workingSet) freeInput(n int64) {
	m.mu.Lock()
	m.input -= n
	m.mu.Unlock()
}

// Validate the complete JSON without Decoder retaining a whole ignored value.
// The mapping stays accounted by the caller until it is unmapped.
func validSpoolJSON(ctx context.Context, raw *spool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	mapped, err := unix.Mmap(int(raw.File.Fd()), 0, int(raw.offset+raw.size), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		return err
	}
	valid := json.Valid(mapped[raw.offset:])
	unmapErr := unix.Munmap(mapped)
	if !valid {
		return errJSONSyntax
	}
	if unmapErr != nil {
		return unmapErr
	}
	return ctx.Err()
}

func inputParseCredit(raw *spool) int64 {
	return 2*raw.size + parseMetadataCredit
}

func spoolFlags(ctx context.Context, raw *spool) (stream, background bool, err error) {
	release, err := raw.mem.reserveInput(inputParseCredit(raw))
	if err != nil {
		return false, false, err
	}
	defer release()
	if err := validSpoolJSON(ctx, raw); err != nil {
		return false, false, err
	}
	err = spoolScanner(raw).objectFields(raw.keySlice, func(key string, start, end int64) error {
		switch key {
		case "stream":
			stream = string(raw.keySlice(start, end)) == "true"
		case "background":
			background = string(raw.keySlice(start, end)) == "true"
		}
		return nil
	})
	return stream, background, err
}

// Only actual cancellation sets an immediate selected-stream write deadline.
// Cleanup joins callbacks before resetting it, so no callback can poison reuse.
func interruptCancelledWrites(w http.ResponseWriter, contexts ...context.Context) (func(), error) {
	var cancellable []context.Context
	for _, ctx := range contexts {
		if ctx != nil && ctx.Done() != nil {
			cancellable = append(cancellable, ctx)
		}
	}
	if len(cancellable) == 0 {
		return func() {}, nil
	}
	controller := http.NewResponseController(w)
	if err := controller.SetWriteDeadline(time.Time{}); err != nil {
		return nil, err
	}
	type callback struct {
		stop func() bool
		done chan struct{}
	}
	callbacks := make([]callback, 0, len(cancellable))
	for _, ctx := range cancellable {
		done := make(chan struct{})
		stop := context.AfterFunc(ctx, func() {
			defer close(done)
			_ = controller.SetWriteDeadline(time.Now())
		})
		callbacks = append(callbacks, callback{stop, done})
	}
	return func() {
		for _, cb := range callbacks {
			if !cb.stop() {
				<-cb.done
			}
		}
		_ = controller.SetWriteDeadline(time.Time{})
	}, nil
}

func writeCancellationError(w http.ResponseWriter, err error) {
	if errors.Is(err, http.ErrNotSupported) {
		writeAPIError(w, &apiError{status: http.StatusBadGateway, typ: "server_error", code: "write_cancellation_unsupported", message: "Response transport does not support cancellation."})
		return
	}
	writeAPIError(w, workingError(err))
}

// Reader consumers must still propagate cancellation while copying disk spans.
type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}

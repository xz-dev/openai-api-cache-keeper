package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Bounds for the unchanged 384-MiB envelope. Slow upstreams retain disk files,
// while simultaneously materialized scratch is admitted by actual byte credits.
const (
	// The expanded history spool follows the 256 MiB end-to-end cap (design
	// D1); it lives on disk, never in the heap.
	maxHistoryBytes int64 = 256 << 20
	maxEventBytes         = 1 << 20
	// maxResponseItems bounds the output array of ONE upstream response
	// object (itself bounded by maxResponseBytes) so its slice header is a
	// fixed term of responseScratch. History and request input have no item
	// cap (design D5).
	maxResponseItems   = 8192
	maxResponseBytes   = 8 << 20 // per object, event or non-stream body
	maxCollectedBytes  = 16 << 20
	maxTaskStreamBytes = 8 << 20 // cumulative task-log re-parse budget; SSE relay itself is unbounded
	// maxSpoolBytes is keeper's transient disk byte budget: one maximum
	// turn holds its raw request and its rebuilt upstream spool (k_disk 2,
	// design D4) with room for concurrent small turns.
	maxSpoolBytes int64 = 4 * maxHistoryBytes
)

var errWorkSize = errors.New("encoded history or response exceeds working limit")

// sizeLimitError is a size refusal that names the hop, the limit and its
// setting (design D7); it still matches errWorkSize.
type sizeLimitError struct {
	code, what, setting string
	limit               int64
}

func (e *sizeLimitError) Error() string {
	return fmt.Sprintf("keeper: %s exceeds the %d-byte limit (%s)", e.what, e.limit, e.setting)
}

func (e *sizeLimitError) Is(target error) bool { return target == errWorkSize }

var errHistorySize error = &sizeLimitError{code: "history_too_large", what: "the expanded history", setting: "keeper history limit", limit: maxHistoryBytes}

func requestSizeError(limit int64) error {
	return &sizeLimitError{code: "request_too_large", what: "the request body or frame", setting: "MAX_REQUEST_BYTES", limit: limit}
}

// Output and input scratch have independent, non-waiting byte accounting.
// Fixed connection admission remains until its lifecycle-credit slice lands.
type workingSet struct {
	activeTurns   atomic.Int64
	activeSockets atomic.Int64
	activeTasks   atomic.Int64
	mu            sync.Mutex
	disk          int64
	output        int64
	input         int64
	statfs        func(fd int, st *unix.Statfs_t) error // free-space source; nil = unix.Fstatfs
}

func newWorkingSet() *workingSet { return &workingSet{} }

// previousNotFound never echoes an unbounded client id: the message carries
// at most a 64-byte UTF-8-safe prefix (the lookup itself used the full id).
func previousNotFound(id string) *apiError {
	if len(id) > 64 {
		cut := 64
		for cut > 0 && !utf8.RuneStart(id[cut]) {
			cut--
		}
		id = id[:cut] + "..."
	}
	return &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", code: "previous_response_not_found", param: "previous_response_id",
		message: fmt.Sprintf("Previous response with id '%s' not found.", id)}
}

func workingError(err error) *apiError {
	var tooLarge *http.MaxBytesError
	var named *sizeLimitError
	if errors.As(err, &tooLarge) {
		err = requestSizeError(tooLarge.Limit)
	}
	if errors.As(err, &named) {
		return &apiError{status: 413, typ: "invalid_request_error", code: named.code, message: named.Error()}
	}
	var refused *authRefusal
	if errors.As(err, &refused) {
		return refused.e
	}
	var floor *storageFloorError
	if errors.As(err, &floor) {
		return &apiError{status: http.StatusInsufficientStorage, typ: "server_error", code: "insufficient_storage", message: floor.Error()}
	}
	if errors.Is(err, errWorkSize) {
		return &apiError{status: 413, typ: "invalid_request_error", code: "working_set_too_large", message: err.Error()}
	}
	return &apiError{status: 503, typ: "server_error", code: "working_set_unavailable", message: "Working capacity unavailable; retry later."}
}

// spoolRequest copies one request body or message to a new spool. auth,
// when set, authenticates the caller once 64 MiB have arrived (design D4).
func (k *keeper) spoolRequest(ctx context.Context, r io.Reader, auth largeAuth) (*spool, error) {
	release, err := k.work.reserveInput(requestCopyCredit)
	if err != nil {
		return nil, err
	}
	defer release()
	f, err := k.newSpool()
	if err != nil {
		return nil, err
	}
	// Hide os.File.ReadFrom so CopyBuffer cannot bypass byte accounting.
	n, err := copyAuthenticated(ctx, struct{ io.Writer }{f}, contextReader{ctx, r}, k.cfg.maxRequestBytes+1, auth, make([]byte, 32<<10))
	if err == nil && n > k.cfg.maxRequestBytes {
		err = requestSizeError(k.cfg.maxRequestBytes)
	}
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

type itemSpan struct {
	off int64
	n   int
}

// diskTurn is a spooled upstream request plus the file spans of every history
// item in storage order. Heap cost is O(item count), never O(history bytes).
type diskTurn struct {
	file           *spool
	spans          []itemSpan
	store, hasPrev bool
	outStart       int // index of first output span; spans[:outStart] is input
	spanFree       func()
}

// itemReader streams each span through a small reusable buffer.
func (t *diskTurn) itemSource() itemFactory {
	return func() itemSource { return t.itemReader() }
}

func (t *diskTurn) itemReader() itemSource {
	i := 0
	var buf []byte
	var free func()
	finish := func() {
		buf = nil
		if free != nil {
			free()
			free = nil
		}
	}
	return func(dst []byte) ([]byte, error) {
		if i >= len(t.spans) {
			finish()
			return nil, io.EOF
		}
		span := t.spans[i]
		i++
		if cap(buf) < span.n {
			release, err := t.file.mem.reserveInput(int64(span.n))
			if err != nil {
				finish()
				return nil, err
			}
			next := make([]byte, span.n)
			finish()
			buf, free = next, release
			t.file.mu.Lock()
			t.file.inputCredits = append(t.file.inputCredits, release)
			t.file.mu.Unlock()
		}
		need := buf[:span.n]
		if _, err := t.file.ReadAt(need, span.off); err != nil {
			finish()
			return nil, err
		}
		return need, nil
	}
}

// history materializes all spans; used by tests and small callers only.
func (t *diskTurn) history() ([]json.RawMessage, error) {
	full := make([]json.RawMessage, 0, len(t.spans))
	for _, span := range t.spans {
		raw := make(json.RawMessage, span.n)
		if _, err := t.file.ReadAt(raw, span.off); err != nil {
			return nil, err
		}
		full = append(full, raw)
	}
	return full, nil
}

// prepareDisk parses the spooled request one field at a time. Input items
// are decoded directly off the file and appended to the upstream spool as
// spans; a previous_response_id expands by streaming stored items through
// short read txs into the same spool. Heap stays O(one item).
func (k *keeper) prepareDisk(ctx context.Context, raw *spool, owner [32]byte, lookup func(id string) (tip [32]byte, found bool), ws bool) (*diskTurn, *apiError) {
	return k.prepareDiskEnv(ctx, raw, owner, lookup, ws, false)
}

// prepareDiskEnv is prepareDisk; with wsEnvelope the rebuilt spool keeps a
// `"type":"response.create"` field so the result is a valid upstream
// WebSocket frame instead of a bare HTTP request body.
// fieldSpan is one retained top-level request field's raw value span in the
// request spool; values stream from the file, never through the heap.
type fieldSpan struct {
	key        string
	start, end int64
}

// prepareDiskEnv is prepareDisk; with wsEnvelope the rebuilt spool keeps a
// `"type":"response.create"` field so the result is a valid upstream
// WebSocket frame instead of a bare HTTP request body. It scans the spooled
// request one field at a time with the span scanner: input items and every
// other field value stay file spans, so a multi-megabyte input never
// materializes or re-encodes through the heap (REVIEW3 B6).
func (k *keeper) prepareDiskEnv(ctx context.Context, raw *spool, owner [32]byte, lookup func(id string) (tip [32]byte, found bool), ws, wsEnvelope bool) (*diskTurn, *apiError) {
	return k.prepareDiskSnapshot(ctx, raw, owner, func(id string) (*historySnapshot, error) {
		tip, found := lookup(id)
		if !found {
			return nil, errMissingItem
		}
		return k.store.snapshotTip(ctx, tip, owner)
	}, ws, wsEnvelope)
}

func (k *keeper) prepareDiskSnapshot(ctx context.Context, raw *spool, owner [32]byte, lookup func(string) (*historySnapshot, error), ws, wsEnvelope bool) (*diskTurn, *apiError) {
	releaseInput, err := k.work.reserveInput(inputParseCredit(raw))
	if err != nil {
		return nil, workingError(err)
	}
	defer releaseInput()
	if err := validSpoolJSON(ctx, raw); err != nil {
		return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
	}
	if err := ctx.Err(); err != nil {
		return nil, workingError(err)
	}
	task, _ := ctx.Value(backgroundTaskKey{}).(*responseTask)
	t := &diskTurn{store: true}
	var prevID string
	var prevStart, prevEnd int64 // the client's previous_response_id token span
	var readErr error
	var prevErr bool
	var selected *historySnapshot
	defer func() {
		if selected != nil {
			_ = selected.Close()
		}
	}()
	// Field spans forwarded to the upstream body: model first, then input,
	// then the remaining fields sorted by key.
	var other []fieldSpan
	seen := map[string]int{}
	var frees []func()
	defer func() {
		for _, free := range frees {
			free()
		}
	}()
	charge := func(n int64) error {
		free, err := k.work.reserveInput(n)
		if err == nil {
			frees = append(frees, free)
		}
		return err
	}
	putOther := func(name string, start, end int64) error {
		if i, ok := seen[name]; ok {
			other[i] = fieldSpan{name, start, end}
			return nil
		}
		// Top-level field count is bounded only by bytes: charge each new
		// field's slice slot (with doubling overlap), the seen/spans map
		// entries, the keys slot, the name, and its JSON re-encode buffer
		// (6x escape, doubled by bytes.Buffer growth).
		if err := charge(5*int64(unsafe.Sizeof(fieldSpan{})) + 128 + 14*heapBytes(len(name))); err != nil {
			readErr = err
			return err
		}
		seen[name] = len(other)
		other = append(other, fieldSpan{name, start, end})
		return nil
	}
	// Input item spans recorded as offsets in the RAW request file; nothing
	// is copied until expansion writes the upstream spool in history order.
	var inSpans []itemSpan
	var inputIsString bool
	var inputSpan fieldSpan
	var inputNull, inputErr bool
	scanErr := spoolScanner(raw).objectFields(raw.keySlice, func(name string, start, end int64) error {
		switch name {
		case "prompt_cache_retention":
			// Routed mode drops this field on ordinary turns; the pure
			// single-upstream cache is not a policy filter and forwards it.
			if task != nil || k.cfg.upstream != nil {
				return putOther(name, start, end)
			}
		case "input":
			b := raw.keySlice(start, min(end, start+1))
			if len(b) == 0 {
				inputErr = true
				return nil
			}
			switch b[0] {
			case '[':
				// Item spans relative to the value's own start.
				sub := newJSONScanner(raw.spanReader(start, end))
				err := sub.arrayItems(func(vs, ve int64) error {
					next, _, err := growSpans(inSpans, raw)
					if err != nil {
						return err
					}
					inSpans = append(next, itemSpan{start + vs, int(ve - vs)})
					return nil
				})
				if errors.Is(err, errWorkSize) {
					// A working-capacity refusal is not malformed input.
					readErr = err
					return err
				}
				if err != nil {
					inputErr = true
				}
			case '"':
				inputIsString = true
				inputSpan = fieldSpan{"input", start, end}
			case 'n': // null
				inputNull = true
			default:
				inputErr = true
			}
		case "previous_response_id":
			// Any-size ID decoded straight from the spool (no raw copy, invalid
			// UTF-8 refused): charge the id, the store lookup's key copy and
			// the decoder's read buffer.
			if end-start == 4 && string(raw.keySlice(start, end)) == "null" {
				return nil
			}
			if err := charge(2*heapBytes(int(end-start)) + 8<<10); err != nil {
				readErr = err
				return err
			}
			id, ok, err := raw.decodeJSONSpan(start, end)
			if err != nil {
				readErr = err
				return err
			}
			if !ok || id == "" {
				prevErr = true
				return nil
			}
			prevID, prevStart, prevEnd = id, start, end
			t.hasPrev = true
		case "type":
			v := raw.keySlice(start, end)
			if ws {
				var kind string
				_ = json.Unmarshal(v, &kind)
				if kind != "response.create" {
					inputErr = true
					return nil
				}
				if wsEnvelope {
					return putOther(name, start, end)
				}
				return nil
			}
			return putOther(name, start, end)
		case "store":
			if v := raw.keySlice(start, end); v != nil {
				t.store = string(v) != "false"
			}
			return putOther(name, start, end)
		default:
			return putOther(name, start, end)
		}
		return nil
	})
	if readErr != nil {
		return nil, workingError(readErr)
	}
	if scanErr != nil {
		if scanErr == errWorkSize {
			return nil, workingError(errWorkSize)
		}
		return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
	}
	if inputErr || prevErr {
		param := "input"
		if !inputErr {
			param = "previous_response_id"
		}
		return nil, &apiError{status: 400, typ: "invalid_request_error", param: param, message: "input must be a string or an array of items"}
	}
	_ = inputNull
	// String input becomes a user item; stream {"role":"user","content":<raw string>}
	// with the raw string bytes copied verbatim (already valid JSON), so a
	// multi-megabyte string is never decoded or re-encoded through the heap.
	if inputIsString {
		// handled below: write wrapper around the raw string span
	}
	// Synthesized fields not present as file spans (background removal,
	// native-parent prev forwarding, ws stream injection) are tiny by
	// construction and kept as raw JSON.
	extra := map[string]json.RawMessage{}
	drop := map[string]bool{}
	if task != nil {
		drop["background"] = true
		if prevID != "" {
			var err error
			selected, err = lookup(prevID)
			if err != nil && !errors.Is(err, errMissingItem) {
				return nil, workingError(err)
			}
			if errors.Is(err, errMissingItem) && !strings.HasPrefix(prevID, keeperResponsePrefix) {
				// Forward the client's token itself (already valid JSON, never
				// re-encoded), re-read under its own credit while it is held.
				freeToken, err := k.work.reserveInput(heapBytes(int(prevEnd - prevStart)))
				if err != nil {
					return nil, workingError(err)
				}
				defer freeToken()
				token, err := raw.spanBytes(prevStart, prevEnd)
				if err != nil {
					return nil, workingError(err)
				}
				extra["previous_response_id"] = token
				prevID = ""
				task.mu.Lock()
				task.nativeParent = true
				task.mu.Unlock()
			}
		}
	}
	if ws && !wsEnvelope {
		extra["stream"] = json.RawMessage("true")
	}
	fail := func(err error) (*diskTurn, *apiError) {
		if t.file != nil {
			t.file.Close()
		}
		return nil, workingError(err)
	}
	// Upstream spool: {"model":..., "input":[ <history> , <input items> ], ...fields }
	f, err := k.newSpool()
	if err != nil {
		return nil, workingError(err)
	}
	t.file = f
	out := &spoolWriter{f: f, limit: maxHistoryBytes}
	copyBuf := make([]byte, 32<<10)
	if err := out.write([]byte(`{`)); err != nil {
		return fail(err)
	}
	if i, ok := seen["model"]; ok {
		if err := out.write([]byte(`"model":`)); err != nil {
			return fail(err)
		}
		fs := other[i]
		if _, err := io.CopyBuffer(struct{ io.Writer }{f}, raw.spanReader(fs.start, fs.end), copyBuf); err != nil {
			return fail(err)
		}
		if f.size > maxHistoryBytes {
			return fail(errHistorySize)
		}
		if err := out.write([]byte(`,`)); err != nil {
			return fail(err)
		}
		drop["model"] = true
	}
	if err := out.write([]byte(`"input":[`)); err != nil {
		return fail(err)
	}
	nItems := 0
	if prevID != "" {
		history := selected
		var err error
		if history == nil {
			history, err = lookup(prevID)
		}
		if errors.Is(err, errMissingItem) {
			f.Close()
			return nil, previousNotFound(prevID)
		}
		if err != nil {
			return fail(err)
		}
		err = history.each(ctx, func(size int64, item io.Reader) error {
			var separator int64
			if nItems > 0 {
				separator = 1
			}
			if f.size+size+separator > maxHistoryBytes {
				return errHistorySize
			}
			if nItems > 0 {
				if err := out.write([]byte(",")); err != nil {
					return err
				}
			}
			off := f.size
			if _, err := io.CopyBuffer(struct{ io.Writer }{f}, item, copyBuf); err != nil {
				return err
			}
			if err := t.addSpan(itemSpan{off, int(size)}); err != nil {
				return err
			}
			nItems++
			return nil
		})
		closeErr := history.Close()
		if err != nil {
			return fail(err)
		}
		if closeErr != nil {
			return fail(closeErr)
		}
	}
	// Client input items: copy spans from the raw request file with a small
	// buffer. A string input wraps its raw bytes verbatim in the user-item
	// envelope — the quoted string needs no decode/re-encode.
	copyItemSpan := func(r io.Reader) error {
		if nItems > 0 {
			if err := out.write([]byte(",")); err != nil {
				return err
			}
		}
		off := f.size
		if _, err := io.CopyBuffer(struct{ io.Writer }{f}, r, copyBuf); err != nil {
			return err
		}
		if f.size-off > maxHistoryBytes {
			return errHistorySize
		}
		if err := t.addSpan(itemSpan{off, int(f.size - off)}); err != nil {
			return err
		}
		nItems++
		return nil
	}
	for _, span := range inSpans {
		if err := copyItemSpan(raw.section(span.off, int64(span.n))); err != nil {
			return fail(err)
		}
	}
	if inputIsString {
		if nItems > 0 {
			if err := out.write([]byte(",")); err != nil {
				return fail(err)
			}
		}
		off := f.size
		if err := out.write([]byte(`{"content":`)); err != nil {
			return fail(err)
		}
		if _, err := io.CopyBuffer(struct{ io.Writer }{f}, raw.spanReader(inputSpan.start, inputSpan.end), copyBuf); err != nil {
			return fail(err)
		}
		if err := out.write([]byte(`,"role":"user"}`)); err != nil {
			return fail(err)
		}
		if f.size-off > maxHistoryBytes {
			return fail(errHistorySize)
		}
		if err := t.addSpan(itemSpan{off, int(f.size - off)}); err != nil {
			return fail(err)
		}
		nItems++
	}
	if err := out.write([]byte("]")); err != nil {
		return fail(err)
	}
	keys := make([]string, 0, len(other)+len(extra))
	spans := map[string]fieldSpan{}
	for _, fs := range other {
		if drop[fs.key] {
			continue
		}
		if _, dup := spans[fs.key]; !dup {
			spans[fs.key] = fs
			keys = append(keys, fs.key)
		}
	}
	for k := range extra {
		if _, dup := spans[k]; !dup {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		var keyJSON bytes.Buffer
		enc := json.NewEncoder(&keyJSON)
		enc.SetEscapeHTML(false)
		if e := enc.Encode(key); e != nil {
			return fail(e)
		}
		name := bytes.TrimRight(keyJSON.Bytes(), "\n")
		if err := out.write([]byte(",")); err != nil {
			return fail(err)
		}
		if err := out.write(name); err != nil {
			return fail(err)
		}
		if err := out.write([]byte(":")); err != nil {
			return fail(err)
		}
		if v, ok := extra[key]; ok {
			if err := out.write(v); err != nil {
				return fail(err)
			}
			continue
		}
		fs := spans[key]
		if _, err := io.CopyBuffer(struct{ io.Writer }{f}, raw.spanReader(fs.start, fs.end), copyBuf); err != nil {
			return fail(err)
		}
		if f.size > maxHistoryBytes {
			return fail(errHistorySize)
		}
	}
	if err := out.write([]byte("}")); err != nil {
		return fail(err)
	}
	if f.size > maxHistoryBytes {
		return fail(errHistorySize)
	}
	if k.cfg.ignoreClientStoreFalse {
		t.store = true
	}
	if task != nil && task.nativeParent {
		// Unknown native parent history must stay native, never a partial local chain.
		t.store = false
	}
	return t, nil
}

// spoolWriter appends bounded bytes to a spool.
type spoolWriter struct {
	f     *spool
	limit int64
}

func (w *spoolWriter) write(p []byte) error {
	if w.f.size+int64(len(p)) > w.limit {
		return errHistorySize
	}
	_, err := w.f.Write(p)
	return err
}

// acceptDisk streams the turn's history spans through the store's plan/link
// path. The encoded history (spans + separators + 2) must fit the replay
// limit BEFORE staging/linking; overflow rejects the turn so a published
// alias/pin can always be expanded on the next call. Input bytes have independent
// credits; actual store mutation is serialized with cancellable queued admission.
type diskAcceptance struct {
	k       *keeper
	turn    *diskTurn
	id      string
	owner   [32]byte
	oldPin  string
	pin     bool
	encoded int64
	state   *stateLease
}

func (p *diskAcceptance) close() { p.state.Close() }
func (p *diskAcceptance) commit(ctx context.Context) (string, error) {
	_, handle, err := p.k.store.acceptWithBudget(ctx, p.id, p.owner, p.turn.itemSource(), p.oldPin, p.turn.store, p.pin, func() (int64, func()) { return p.k.reserveHistoryCache(p.encoded) })
	return handle, err
}
func (k *keeper) acceptDisk(ctx context.Context, t *diskTurn, id string, owner [32]byte, output []json.RawMessage, oldPin string, pin bool) (string, error) {
	p, err := k.prepareAcceptDisk(ctx, t, id, owner, output, oldPin, pin)
	output = nil
	if err != nil {
		return "", err
	}
	defer p.close()
	return p.commit(ctx)
}
func (k *keeper) prepareAcceptDisk(ctx context.Context, t *diskTurn, id string, owner [32]byte, output []json.RawMessage, oldPin string, pin bool) (*diskAcceptance, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var encoded int64 = 2
	for _, span := range t.spans {
		encoded += int64(span.n) + 1
	}
	for _, item := range output {
		encoded += int64(len(item)) + 1
	}
	if encoded > maxHistoryBytes {
		return nil, errHistorySize
	}
	// Output items are appended to the turn spool so accept replays everything
	// from disk. Spans already cover input+history; output extends them.
	if len(output) > 0 {
		t.outStart = len(t.spans)
		for _, item := range output {
			if len(item) == 0 {
				continue
			}
			off := t.file.size
			if _, err := t.file.Write(item); err != nil {
				return nil, err
			}
			if err := t.addSpan(itemSpan{off, len(item)}); err != nil {
				return nil, err
			}
		}
	}
	state, err := k.work.state(int64(512+len(id)+len(oldPin)), nil)
	if err != nil {
		return nil, err
	}
	return &diskAcceptance{k: k, turn: t, id: strings.Clone(id), owner: owner, oldPin: strings.Clone(oldPin), pin: pin, encoded: encoded, state: state}, nil
}

func (k *keeper) forwardDisk(ctx context.Context, upstream *url.URL, path, rawQuery string, header http.Header, t *diskTurn) (*http.Response, error) {
	body, err := t.file.requestBody()
	if err != nil {
		return nil, err
	}
	return k.forwardReader(ctx, upstream, path, rawQuery, header, body)
}

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
	"time"
)

// Bounds for the measured 384-MiB envelope. Materialization is a short phase,
// never a whole streamed turn. Slow upstreams retain disk files, not histories.
const (
	maxTurns                 = 64
	maxSockets               = 64
	maxTaskReaders           = 4 // concurrent local readers per background task
	maxHistoryBytes    int64 = 64 << 20
	maxHistoryItems          = 8192
	maxEventBytes            = 1 << 20
	maxResponseBytes         = 8 << 20 // per object, event or non-stream body
	maxCollectedBytes        = 16 << 20
	maxTaskStreamBytes       = 8 << 20 // cumulative task-log re-parse budget; SSE relay itself is unbounded
	maxSpoolBytes      int64 = 1 << 30
	workWait                 = 8 * time.Second
)

var errWorkSize = errors.New("encoded history or response exceeds working limit")

// One short materialization phase serializes input, commit and response
// parse/rewrite. Response buffers/scratch also reserve explicit byte capacity;
// the phase token itself is NOT a byte allocator or an RSS guarantee.
type workingSet struct {
	phase   chan struct{}
	turns   chan struct{}
	sockets chan struct{}
	mu      sync.Mutex
	disk    int64
	output  int64
}

func newWorkingSet() *workingSet {
	return &workingSet{phase: make(chan struct{}, 1), turns: make(chan struct{}, maxTurns), sockets: make(chan struct{}, maxSockets)}
}
func (m *workingSet) enter(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timer := time.NewTimer(workWait)
	defer timer.Stop()
	select {
	case m.phase <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.phase
			return nil, err
		}
		return func() { <-m.phase }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
		return nil, errors.New("working-set wait exceeded")
	}
}
func trySlot(ch chan struct{}) bool {
	select {
	case ch <- struct{}{}:
		return true
	default:
		return false
	}
}
func workingError(err error) *apiError {
	var tooLarge *http.MaxBytesError
	if errors.Is(err, errWorkSize) || errors.As(err, &tooLarge) {
		return &apiError{status: 413, typ: "invalid_request_error", code: "working_set_too_large", message: err.Error()}
	}
	return &apiError{status: 503, typ: "server_error", code: "working_set_unavailable", message: "Working capacity unavailable; retry later."}
}

func (k *keeper) spoolRequest(ctx context.Context, r io.Reader) (*spool, error) {
	f, err := k.newSpool()
	if err != nil {
		return nil, err
	}
	// Hide os.File.ReadFrom so CopyBuffer cannot bypass byte accounting.
	n, err := io.CopyBuffer(struct{ io.Writer }{f}, io.LimitReader(r, k.cfg.maxRequestBytes+1), make([]byte, 32<<10))
	if err == nil && n > k.cfg.maxRequestBytes {
		err = errWorkSize
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
}

// itemReader streams each span through a small reusable buffer.
func (t *diskTurn) itemSource() itemFactory {
	return func() itemSource { return t.itemReader() }
}

func (t *diskTurn) itemReader() itemSource {
	i := 0
	buf := make([]byte, 0, 64<<10)
	return func(dst []byte) ([]byte, error) {
		if i >= len(t.spans) {
			return nil, io.EOF
		}
		span := t.spans[i]
		i++
		need := buf
		if cap(need) < span.n {
			need = make([]byte, span.n)
			buf = need
		}
		need = need[:span.n]
		if _, err := t.file.ReadAt(need, span.off); err != nil {
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
	if err := ctx.Err(); err != nil {
		return nil, workingError(err)
	}
	task, _ := ctx.Value(backgroundTaskKey{}).(*responseTask)
	t := &diskTurn{store: true}
	var prevID string
	// Field spans of everything forwarded to the upstream body, in file order;
	// written back out sorted by key at the end.
	var other []fieldSpan
	seen := map[string]int{}
	putOther := func(name string, start, end int64) {
		if i, ok := seen[name]; ok {
			other[i] = fieldSpan{name, start, end}
			return
		}
		seen[name] = len(other)
		other = append(other, fieldSpan{name, start, end})
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
				putOther(name, start, end)
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
					if len(inSpans) >= maxHistoryItems {
						return errWorkSize
					}
					inSpans = append(inSpans, itemSpan{start + vs, int(ve - vs)})
					return nil
				})
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
			if v := raw.keySlice(start, end); v != nil && string(v) != "null" {
				var id string
				if json.Unmarshal(v, &id) != nil || id == "" {
					inputErr = true
					return nil
				}
				prevID = id
				t.hasPrev = true
			}
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
					putOther(name, start, end)
				}
				return nil
			}
			putOther(name, start, end)
		case "store":
			if v := raw.keySlice(start, end); v != nil {
				t.store = string(v) != "false"
			}
			putOther(name, start, end)
		default:
			putOther(name, start, end)
		}
		return nil
	})
	if scanErr != nil {
		if scanErr == errWorkSize {
			return nil, workingError(errWorkSize)
		}
		return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
	}
	if inputErr {
		return nil, &apiError{status: 400, typ: "invalid_request_error", param: "input", message: "input must be a string or an array of items"}
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
			if _, local := lookup(prevID); !local && !strings.HasPrefix(prevID, keeperResponsePrefix) {
				extra["previous_response_id"], _ = json.Marshal(prevID)
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
	// Upstream spool: {"input":[ <history> , <input items> ], ...fields }
	f, err := k.newSpool()
	if err != nil {
		return nil, workingError(err)
	}
	t.file = f
	out := &spoolWriter{f: f, limit: maxHistoryBytes}
	if err := out.write([]byte(`{"input":[`)); err != nil {
		return fail(err)
	}
	nItems := 0
	putItem := func(item []byte) error {
		if nItems >= maxHistoryItems {
			return errWorkSize
		}
		if nItems > 0 {
			if err := out.write([]byte(",")); err != nil {
				return err
			}
		}
		off := f.size
		if err := out.write(item); err != nil {
			return err
		}
		t.spans = append(t.spans, itemSpan{off, len(item)})
		nItems++
		return nil
	}
	if prevID != "" {
		tip, found := lookup(prevID)
		if !found {
			f.Close()
			return nil, &apiError{status: 400, typ: "invalid_request_error", code: "previous_response_not_found", param: "previous_response_id", message: fmt.Sprintf("Previous response with id '%s' not found.", prevID)}
		}
		hashes, err := k.store.tipHashes(tip, owner)
		if err == errMissingItem {
			// A foreign owner's alias resolves but its chain does not.
			f.Close()
			return nil, &apiError{status: 400, typ: "invalid_request_error", code: "previous_response_not_found", param: "previous_response_id", message: fmt.Sprintf("Previous response with id '%s' not found.", prevID)}
		}
		if err != nil {
			return fail(err)
		}
		// Stream stored items from mmap into the spool via short read txs.
		if _, _, err := k.store.writeHistoryTo(hashes, putItem); err != nil {
			return fail(err)
		}
	}
	// Client input items: copy spans from the raw request file with a small
	// buffer. A string input wraps its raw bytes verbatim in the user-item
	// envelope — the quoted string needs no decode/re-encode.
	copyBuf := make([]byte, 32<<10)
	copyItemSpan := func(r io.Reader) error {
		if nItems >= maxHistoryItems {
			return errWorkSize
		}
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
			return errWorkSize
		}
		t.spans = append(t.spans, itemSpan{off, int(f.size - off)})
		nItems++
		return nil
	}
	for _, span := range inSpans {
		if err := copyItemSpan(io.NewSectionReader(raw.File, raw.offset+span.off, int64(span.n))); err != nil {
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
			return fail(errWorkSize)
		}
		t.spans = append(t.spans, itemSpan{off, int(f.size - off)})
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
			return fail(errWorkSize)
		}
	}
	if err := out.write([]byte("}")); err != nil {
		return fail(err)
	}
	if f.size > maxHistoryBytes {
		return fail(errWorkSize)
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
		return errWorkSize
	}
	_, err := w.f.Write(p)
	return err
}

// acceptDisk streams the turn's history spans through the store's plan/link
// path. The encoded history (spans + separators + 2) must fit the replay
// limit BEFORE staging/linking; overflow rejects the turn so a published
// alias/pin can always be expanded on the next call. Caller holds the short
// materialization phase; no nested acquisition.
func (k *keeper) acceptDisk(ctx context.Context, t *diskTurn, id string, owner [32]byte, output []json.RawMessage, oldPin string, pin bool) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	var encoded int64 = 2
	for _, span := range t.spans {
		encoded += int64(span.n) + 1
	}
	for _, item := range output {
		encoded += int64(len(item)) + 1
	}
	if encoded > maxHistoryBytes || len(t.spans)+len(output) > maxHistoryItems {
		return "", errWorkSize
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
				return "", err
			}
			t.spans = append(t.spans, itemSpan{off, len(item)})
		}
	}
	if len(t.spans) > maxHistoryItems {
		return "", errWorkSize
	}
	k.cacheMu.Lock()
	defer k.cacheMu.Unlock()
	if k.eventBytes > 0 {
		_, history := k.store.stats()
		if history+k.eventBytes+encoded > k.cfg.maxStoreBytes {
			return "", errWorkSize
		}
	}
	_, handle, err := k.store.accept(id, owner, t.itemSource(), oldPin, t.store, pin)
	return handle, err
}

func (k *keeper) forwardDisk(ctx context.Context, upstream *url.URL, path, rawQuery string, header http.Header, t *diskTurn) (*http.Response, error) {
	body, err := t.file.requestBody()
	if err != nil {
		return nil, err
	}
	return k.forwardReader(ctx, upstream, path, rawQuery, header, body)
}

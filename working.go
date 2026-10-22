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
	maxTurns                = 64
	maxSockets              = 64
	maxTaskReaders          = 4 // concurrent local readers per background task
	maxHistoryBytes   int64 = 64 << 20
	maxHistoryItems         = 8192
	maxEventBytes           = 1 << 20
	maxResponseBytes        = 8 << 20
	maxCollectedBytes       = 16 << 20
	maxSpoolBytes     int64 = 1 << 30
	workWait                = 8 * time.Second
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
	if err := ctx.Err(); err != nil {
		return nil, workingError(err)
	}
	task, _ := ctx.Value(backgroundTaskKey{}).(*responseTask)
	r := io.NewSectionReader(raw.File, raw.offset, raw.size)
	dec := json.NewDecoder(r)
	start, decodeErr := dec.Token()
	if decodeErr != nil || start != json.Delim('{') {
		return nil, &apiError{status: 400, typ: "invalid_request_error", message: "request must be a JSON object"}
	}
	t := &diskTurn{store: true}
	var prevID string
	var other map[string]json.RawMessage
	fields := 0
	// Input item spans are recorded as offsets in the RAW request file
	// (dec.InputOffset is the end of the just-decoded value). Nothing is
	// copied until expansion writes the upstream spool in history order.
	var inSpans []itemSpan
	var inStrings []string
	var inputIsString bool
	var inputStr string
	for dec.More() {
		if fields >= 256 {
			return nil, workingError(errWorkSize)
		}
		fields++
		key, e := dec.Token()
		if e != nil {
			return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
		}
		name, ok := key.(string)
		if !ok || len(name) > 64<<10 {
			return nil, workingError(errWorkSize)
		}
		switch name {
		case "prompt_cache_retention":
			if task != nil {
				var value json.RawMessage
				if err := dec.Decode(&value); err != nil {
					return nil, responseError(errUpstreamData)
				}
				if other == nil {
					other = map[string]json.RawMessage{}
				}
				other[name] = value
			} else if _, err := skipJSONValue(dec); err != nil {
				return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
			}
		case "input":
			tok, err := dec.Token()
			if err != nil {
				return nil, &apiError{status: 400, typ: "invalid_request_error", param: "input", message: err.Error()}
			}
			if delim, ok := tok.(json.Delim); ok && delim == '[' {
				var last json.RawMessage
				for dec.More() {
					if len(inSpans) >= maxHistoryItems {
						return nil, workingError(errWorkSize)
					}
					var item json.RawMessage
					if err := dec.Decode(&item); err != nil {
						return nil, &apiError{status: 400, typ: "invalid_request_error", param: "input", message: err.Error()}
					}
					// dec.InputOffset() is the offset AFTER item; the item's
					// own start is len(item) bytes before it (RawMessage keeps
					// raw encoding; whitespace may differ, so re-derive from
					// the decoder's recorded offset minus raw length).
					end := dec.InputOffset()
					inSpans = append(inSpans, itemSpan{end - int64(len(item)), len(item)})
					last = item
				}
				_ = last
				if _, err := dec.Token(); err != nil {
					return nil, &apiError{status: 400, typ: "invalid_request_error", param: "input", message: err.Error()}
				}
			} else if s, ok := tok.(string); ok {
				inputIsString = true
				inputStr = s
			} else if tok == nil {
				// null input
			} else {
				return nil, &apiError{status: 400, typ: "invalid_request_error", param: "input", message: "input must be a string or an array of items"}
			}
		case "previous_response_id":
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil || string(v) == "null" {
				continue
			}
			var id string
			if json.Unmarshal(v, &id) != nil || id == "" {
				return nil, &apiError{status: 400, typ: "invalid_request_error", param: "previous_response_id", message: "previous_response_id must be a non-empty string"}
			}
			prevID = id
			t.hasPrev = true
		case "type":
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil {
				return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
			}
			if ws {
				var kind string
				_ = json.Unmarshal(v, &kind)
				if kind != "response.create" {
					return nil, &apiError{status: 400, typ: "invalid_request_error", param: "type", message: "unsupported event type"}
				}
				continue
			}
			if other == nil {
				other = map[string]json.RawMessage{}
			}
			other[name] = v
		case "store":
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil {
				return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
			}
			t.store = string(v) != "false"
			if other == nil {
				other = map[string]json.RawMessage{}
			}
			other[name] = v
		default:
			var v json.RawMessage
			if err := dec.Decode(&v); err != nil {
				return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
			}
			if other == nil {
				other = map[string]json.RawMessage{}
			}
			other[name] = v
		}
	}
	if _, e := dec.Token(); e != nil {
		return nil, &apiError{status: 400, typ: "invalid_request_error", message: "invalid request JSON"}
	}
	var trailing json.RawMessage
	if e := dec.Decode(&trailing); e != io.EOF {
		return nil, &apiError{status: 400, typ: "invalid_request_error", message: "request must contain one JSON object"}
	}
	if inputIsString {
		var sb bytes.Buffer
		enc := json.NewEncoder(&sb)
		enc.SetEscapeHTML(false)
		if err := enc.Encode(map[string]any{"role": "user", "content": inputStr}); err != nil {
			return nil, &apiError{status: 400, typ: "invalid_request_error", param: "input", message: err.Error()}
		}
		inStrings = append(inStrings, string(bytes.TrimRight(sb.Bytes(), "\n")))
	}
	if task != nil {
		delete(other, "background")
		if prevID != "" {
			if _, local := lookup(prevID); !local && !strings.HasPrefix(prevID, keeperResponsePrefix) {
				if other == nil {
					other = map[string]json.RawMessage{}
				}
				other["previous_response_id"], _ = json.Marshal(prevID)
				prevID = ""
				task.mu.Lock()
				task.nativeParent = true
				task.mu.Unlock()
			}
		}
	}
	if ws {
		if other == nil {
			other = map[string]json.RawMessage{}
		}
		other["stream"] = json.RawMessage("true")
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
	// buffer. String input already marshaled to inStrings[0].
	copyBuf := make([]byte, 32<<10)
	for _, span := range inSpans {
		if nItems >= maxHistoryItems {
			return fail(errWorkSize)
		}
		if nItems > 0 {
			if err := out.write([]byte(",")); err != nil {
				return fail(err)
			}
		}
		off := f.size
		if _, err := io.CopyBuffer(struct{ io.Writer }{f}, io.NewSectionReader(raw.File, raw.offset+span.off, int64(span.n)), copyBuf); err != nil {
			return fail(err)
		}
		if f.size-off > maxHistoryBytes {
			return fail(errWorkSize)
		}
		t.spans = append(t.spans, itemSpan{off, int(f.size - off)})
		nItems++
	}
	for _, s := range inStrings {
		if err := putItem([]byte(s)); err != nil {
			return fail(err)
		}
	}
	if err := out.write([]byte("]")); err != nil {
		return fail(err)
	}
	keys := make([]string, 0, len(other))
	for k := range other {
		keys = append(keys, k)
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
		if err := out.write(other[key]); err != nil {
			return fail(err)
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

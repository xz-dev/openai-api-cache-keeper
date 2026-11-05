package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type keeper struct {
	cfg                 config
	store               *store
	client              *http.Client
	routing             *routingClient
	work                *workingSet
	native              *httputil.ReverseProxy
	gate                *capacityGate
	cacheMu             sync.Mutex
	tasks               map[string]*responseTask
	eventBytes          int64
	cachePending        int64 // event-log bytes reserved before slow I/O
	cacheHistoryReserve int64 // active writer's complete replacement live-byte ceiling
}

func newKeeper(cfg config) *keeper {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableCompression = true // SSE must arrive as identity so events can be parsed
	st, err := newStore(cfg.storeDir, cfg.ttl, cfg.maxStoreBytes, cfg.storeMetaBytes, cfg.storePinBytes, cfg.storeCompactBytes)
	if err != nil {
		log.Fatalf("store: %v", err)
	}
	st.maxEntries = cfg.maxStoreEntries
	if err := checkSpools(cfg.storeDir); err != nil {
		st.close()
		log.Fatalf("spools: %v", err)
	}
	ledgerDir, memInfo := cfg.ledgerDir, cfg.memInfoPath
	if ledgerDir == "" {
		ledgerDir = filepath.Join(cfg.storeDir, "capacity-ledger")
	}
	if memInfo == "" {
		memInfo = "/proc/meminfo"
	}
	gate, err := newCapacityGate(ledgerDir, filepath.Join(cfg.storeDir, "keeper.db"), cfg.maxStoreBytes, memInfo)
	if err != nil {
		st.close()
		log.Fatalf("capacity ledger: %v", err)
	}
	var routing *routingClient
	if cfg.routingIndex != nil {
		routing = newRoutingClient(cfg.routingIndex)
	}
	k := &keeper{
		routing: routing,
		gate:    gate,
		work:    newWorkingSet(),
		cfg:     cfg,
		store:   st,
		tasks:   make(map[string]*responseTask),
		client:  &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	}
	st.inputWork = k.work
	if cfg.upstream != nil {
		k.native = k.newNativeProxy()
	}
	return k
}

func (k *keeper) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
		return
	}
	if r.URL.Path == "/metrics/capacity" && r.Method == http.MethodGet {
		k.gate.serveTickets(w)
		return
	}
	// Only keeper issues tickets: an inbound ticket is never trusted (D4).
	r.Header.Del(ticketHeader)
	ctx, scope := withGateScope(r.Context(), k.gate)
	r = r.WithContext(ctx)
	defer scope.done()
	if k.cfg.upstream != nil && k.taskOperation(w, r) {
		return
	}
	if r.URL.Path != "/v1/responses" && r.URL.Path != "/responses" {
		// Native default path: the fixed single upstream decides every other
		// path, method, and query itself. Routed deployments have no single
		// upstream, so nothing transparent exists to forward to.
		if k.cfg.upstream == nil {
			writeAPIError(w, &apiError{status: http.StatusNotImplemented, typ: "invalid_request_error", message: "no upstream configured"})
			return
		}
		k.serveNative(w, r)
		return
	}
	if k.cfg.upstream == nil {
		// Routed mode: every Responses request is a local checkpoint turn.
		k.serveLocalTurn(w, r)
		return
	}
	// Standard Responses paths are recorded locally unless they reference a
	// previous_response_id the keeper cannot expand: such a delta's full input
	// is known only downstream, so it stays native transparent traffic. A
	// keeper-owned id that is expired, foreign or malformed fails closed before
	// the dispatcher returns. WS upgrades are terminated and relayed frame by
	// frame by serveWSNative so the same per-turn rule applies.
	if isUpgradeRequest(r) && r.Method == http.MethodGet {
		k.serveWSNative(w, r)
		return
	}
	if r.Method == http.MethodPost && !isUpgradeRequest(r) {
		release, local, background, handled := k.localCheckpointTurn(w, r)
		if handled {
			return
		}
		defer release()
		if background {
			k.startBackground(w, r)
			return
		}
		if local {
			k.serveLocalTurn(w, r)
			return
		}
	}
	k.serveNative(w, r)
}

// serveLocalTurn dispatches the existing local-history handlers: the WS
// protocol for upgrades, the SSE/JSON relay for POSTs, and the prior explicit
// method error for anything else on these two paths.
func (k *keeper) serveLocalTurn(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodGet && isUpgradeRequest(r):
		k.serveWS(w, r)
	case r.Method == http.MethodPost:
		k.serveHTTP(w, r)
	default:
		w.Header().Set("Allow", "GET, POST")
		writeAPIError(w, &apiError{status: http.StatusMethodNotAllowed, typ: "invalid_request_error", message: "method not allowed"})
	}
}

// localCheckpointTurn decides whether this POST is a local turn, i.e. one the
// keeper can expand and record: no previous_response_id, or one the keeper
// owns for this caller. A previous_response_id the keeper does not know keeps
// the request native so its delta stays meaningful downstream. A keeper-owned
// id that is expired, foreign or malformed fails closed before this returns.
// The body is spooled to inspect the id, then handed back re-read so
// serveHTTP does not re-read the network body; a local turn never reaches
// the native proxy. The returned release keeps the spool alive until the
// chosen handler finishes with the body.
func (k *keeper) localCheckpointTurn(w http.ResponseWriter, r *http.Request) (release func(), local bool, background bool, handled bool) {
	lease, err := k.work.state(turnStateBytes(r.Header), &k.work.activeTurns)
	if err != nil {
		writeAPIError(w, workingError(err))
		return nil, false, false, true
	}
	defer lease.Close()
	if r.ContentLength > k.cfg.maxRequestBytes {
		writeAPIError(w, workingError(requestSizeError(k.cfg.maxRequestBytes)))
		return nil, false, false, true
	}
	auth, apiErr := k.headerAuth(r)
	if apiErr != nil {
		writeAPIError(w, apiErr)
		return nil, false, false, true
	}
	raw, err := k.spoolRequest(r.Context(), http.MaxBytesReader(w, r.Body, k.cfg.maxRequestBytes), auth)
	if err != nil {
		writeAPIError(w, workingError(err))
		return nil, false, false, true
	}
	streamFlag, backgroundFlag, decodeErr := spoolFlags(r.Context(), raw)
	prevID, owned, apiErr := k.ownedLocalPreviousID(raw, k.ownerOf(r))
	if apiErr != nil {
		writeAPIError(w, apiErr)
		raw.Close()
		return nil, false, false, true
	}
	if raw.ticket != nil {
		if e := raw.ticket.resize(raw.size); e != nil { // an unknown length was admitted at the cap
			raw.Close()
			writeAPIError(w, e)
			return nil, false, false, true
		}
	}
	body, err := raw.requestBody()
	if err != nil {
		raw.Close()
		writeAPIError(w, workingError(err))
		return nil, false, false, true
	}
	r.Body = body
	raw.Close() // body holds the spool from here; its handler releases it early
	background = decodeErr == nil && streamFlag && backgroundFlag
	// A turn is local when the keeper can replay its full input: either it
	// carries no parent at all, or it names one the keeper stored for this
	// owner. Any other previous_response_id is an opaque native reference and
	// the request keeps native semantics end to end.
	endState := lease.retain()
	return func() { body.Close(); endState() }, owned || prevID == "", background, false
}

// headerAuth authenticates a declared large body at the headers, before any
// byte is spooled; for an unknown length it returns the step spoolRequest
// runs once 64 MiB have arrived (design D4).
//
// After authentication the request is admitted by the capacity gate: a
// declared body for its length, an unknown length at the cap (D4).
func (k *keeper) headerAuth(r *http.Request) (largeAuth, *apiError) {
	auth := k.largeBodyAuth(r.Header, r.URL.RawQuery)
	scope := gateScopeFrom(r.Context())
	if r.ContentLength < largeBodyBytes {
		return func(ctx context.Context) error {
			if err := auth(ctx); err != nil {
				return err
			}
			if e := scope.ensure(k.cfg.maxRequestBytes); e != nil {
				return &authRefusal{e}
			}
			return nil
		}, nil
	}
	if err := auth(r.Context()); err != nil {
		return nil, workingError(err)
	}
	return nil, scope.ensure(r.ContentLength)
}

// apiError renders as an OpenAI-shaped error object.
type apiError struct {
	status                    int
	typ, code, param, message string
	retryAfter                string // Retry-After hint for capacity_busy
}

func (e *apiError) object() map[string]any {
	obj := map[string]any{"type": e.typ, "message": e.message, "code": nil, "param": nil}
	if e.code != "" {
		obj["code"] = e.code
	}
	if e.param != "" {
		obj["param"] = e.param
	}
	return obj
}

func writeAPIError(w http.ResponseWriter, e *apiError) {
	w.Header().Set("Content-Type", "application/json")
	if e.retryAfter != "" {
		w.Header().Set("Retry-After", e.retryAfter)
	}
	w.WriteHeader(e.status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": e.object()})
}

// failureEvent reports a keeper-side turn failure in Responses protocol terms:
// a request-level error before any response exists, otherwise the terminal
// response.failed, so every client sees the started response end. seq is the
// next event sequence number (0 when the upstream numbered nothing).
func failureEvent(started string, e *apiError, seq int64) map[string]any {
	if started == "" {
		return map[string]any{"type": "error", "error": e.object()}
	}
	return map[string]any{"type": "response.failed", "sequence_number": seq, "response": map[string]any{
		"id": publicID(started), "object": "response", "status": "failed", "output": []any{}, "error": e.object(),
	}}
}

// nextSeq returns the sequence number for a locally generated event,
// continuing whatever the upstream last sent.
func (c *collector) nextSeq() int64 {
	if c.seqSet {
		return c.seq + 1
	}
	return 0
}

// The cache is an optimization: a turn whose history cannot be retained is
// still served, only without a continuable id.
func logNotRetained(err error) {
	log.Printf("history not retained; turn served uncached: %v", err)
}

// ownerOf returns the history owner for a request. In single-upstream mode
// there is no per-credential isolation: the architecture provides it, and
// response ids are unguessable. Routed mode keeps per-credential ownership by
// hashing the caller's credential, matching Sub2API's precedence after the
// same hop-by-hop cleanup used for upstream requests; the digest is not
// authentication.
func (k *keeper) ownerOf(r *http.Request) [32]byte {
	if k.cfg.upstream != nil {
		return [32]byte{}
	}
	h := http.Header{}
	copyHeader(h, r.Header)
	var credential string
	if scheme, token, ok := strings.Cut(h.Get("Authorization"), " "); ok && strings.EqualFold(scheme, "Bearer") {
		credential = strings.TrimSpace(token)
	}
	if credential == "" {
		credential = h.Get("X-API-Key")
	}
	if credential == "" {
		credential = h.Get("X-Goog-API-Key")
	}
	return sha256.Sum256([]byte(credential))
}

type lookupFunc func(id string) ([]json.RawMessage, bool)

// prepare expands previous_response_id into the full input history and drops
// the field, so the upstream never needs to hold state.
func expandInput(doc map[string]json.RawMessage, lookup lookupFunc) ([]json.RawMessage, *apiError) {
	input, err := inputItems(doc["input"])
	if err != nil {
		if errors.Is(err, errWorkSize) {
			return nil, workingError(err)
		}
		return nil, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", param: "input", message: err.Error()}
	}
	if raw, ok := doc["previous_response_id"]; ok && string(raw) != "null" {
		var id string
		if json.Unmarshal(raw, &id) != nil || id == "" {
			return nil, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", param: "previous_response_id", message: "previous_response_id must be a non-empty string"}
		}
		history, found := lookup(id)
		if !found {
			return nil, previousNotFound(id)
		}
		input = append(append(make([]json.RawMessage, 0, len(history)+len(input)), history...), input...)
	}
	delete(doc, "previous_response_id")
	return input, nil
}

// inputItems normalizes Responses input to a list of independent item copies.
func inputItems(raw json.RawMessage) ([]json.RawMessage, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}
	raw = bytes.TrimSpace(raw)
	if len(raw) > 0 && raw[0] == '"' && json.Valid(raw) {
		item := append([]byte(`{"role":"user","content":`), raw...)
		item = append(item, '}')
		return []json.RawMessage{item}, nil
	}
	var items []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	start, err := dec.Token()
	if err != nil || start != json.Delim('[') {
		return nil, errors.New("input must be a string or an array of items")
	}
	for dec.More() {
		var item json.RawMessage
		if err := dec.Decode(&item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if _, err := dec.Token(); err != nil {
		return nil, err
	}
	return items, nil
}

// collector reconstructs the final response from SSE events or a JSON body.
type collector struct {
	pendingState *stateLease
	charged      int64
	items        []json.RawMessage // response.output_item.done, by output_index
	reserve      func(int64) bool  // charges item slots while collect runs
	id           string
	started      string // id from response.created/in_progress
	status       string
	output       []json.RawMessage
	done         bool
	seq          int64 // last upstream sequence_number seen
	seqSet       bool
	rewrite      bool // latched at first response event: this turn publishes resp_ ids
}

// responseID is the upstream id of the response this turn produced, if any;
// response.created is optional, so a collected terminal id also counts.
func (c *collector) responseID() string {
	if c.id != "" {
		return c.id
	}
	return c.started
}

// final returns the response id and output items when the turn may be continued.
func (c *collector) final() (string, []json.RawMessage, bool) {
	if !c.done || (c.status != "completed" && c.status != "incomplete") {
		return "", nil, false
	}
	output := c.output
	if len(output) == 0 { // some upstreams only stream items and send an empty final output
		// Compact in place: the charged slots are reused, nothing allocates.
		kept := c.items[:0]
		for _, item := range c.items {
			if item != nil {
				kept = append(kept, item)
			}
		}
		clear(c.items[len(kept):])
		c.items = kept
		output = kept
	}
	return c.id, output, true
}

// publicID is the id clients see and continue from. Upstream ids that do not
// look like response ids (e.g. Anthropic-style msg_*) get a resp_ prefix, since
// gateways such as Sub2API reject message-like previous_response_id values.
func publicID(id string) string {
	if strings.HasPrefix(id, "resp_") {
		return id
	}
	return "resp_" + id
}

func (k *keeper) forwardReader(ctx context.Context, upstream *url.URL, path, rawQuery string, header http.Header, body io.Reader) (*http.Response, error) {
	target := *upstream
	target.Path = strings.TrimRight(target.Path, "/") + path
	target.RawQuery = rawQuery
	body = retainRequestState(ctx, body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), body)
	if err != nil {
		if closer, ok := body.(io.Closer); ok {
			_ = closer.Close()
		}
		return nil, err
	}
	copyHeader(req.Header, header)
	if k.routing != nil && upstream == k.cfg.upstreamCPA {
		req.Header.Set("Authorization", "Bearer "+k.cfg.cpaAPIKey)
	}
	for name := range req.Header {
		if strings.HasPrefix(strings.ToLower(name), "sec-websocket-") {
			req.Header.Del(name)
		}
	}
	req.Header.Del("Accept-Encoding")
	req.Header.Del("Content-Length")
	req.Header.Set("Content-Type", "application/json")
	return k.client.Do(req)
}

// httpRequestSpool returns the request body's spool and the release of this
// handler's hold on it. A body localCheckpointTurn already spooled (and
// authenticated) is reused, never copied, so keeper's peak disk is the raw
// request plus its expansion, 2N (gate k_disk 2, design D4).
func (k *keeper) httpRequestSpool(w http.ResponseWriter, r *http.Request) (*spool, func(), *apiError) {
	if b, ok := r.Body.(*spoolBody); ok {
		return b.spool, func() { b.Close() }, nil
	}
	auth, apiErr := k.headerAuth(r)
	if apiErr != nil {
		return nil, nil, apiErr
	}
	raw, err := k.spoolRequest(r.Context(), http.MaxBytesReader(w, r.Body, k.cfg.maxRequestBytes), auth)
	if err != nil {
		return nil, nil, workingError(err)
	}
	return raw, func() { raw.Close() }, nil
}

func (k *keeper) serveHTTP(w http.ResponseWriter, r *http.Request) {
	task, _ := r.Context().Value(backgroundTaskKey{}).(*responseTask)
	start := time.Now()
	lease, err := k.work.state(turnStateBytes(r.Header), &k.work.activeTurns)
	if err != nil {
		writeAPIError(w, workingError(err))
		return
	}
	defer lease.Close()
	r = r.WithContext(context.WithValue(r.Context(), requestStateKey{}, lease))
	raw, releaseRaw, apiErr := k.httpRequestSpool(w, r)
	if apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	defer releaseRaw()
	upstream, stream, routeErr := k.routeRequest(r.Context(), raw, false)
	if routeErr != nil {
		writeRoutingError(w, routeErr, stream)
		return
	}
	owner := k.ownerOf(r)
	t, apiErr := k.prepareDiskAdmit(r.Context(), raw, owner, func(id string) (*historySnapshot, error) {
		return k.store.snapshotID(r.Context(), id, owner, false)
	}, false, false, k.admitTurn(r.Context(), raw, k.largeBodyAuth(r.Header, r.URL.RawQuery)))
	releaseRaw()
	if apiErr != nil {
		writeAPIError(w, apiErr)
		return
	}
	defer t.file.Close()
	hasPrev := t.hasPrev
	header := r.Header
	if t.file.ticket != nil {
		header = r.Header.Clone()
		header.Set(ticketHeader, t.file.ticket.id())
	}
	resp, err := k.forwardDisk(r.Context(), upstream, r.URL.Path, r.URL.RawQuery, header, t)
	if err != nil {
		writeAPIError(w, upstreamFailed(requestIDOf(r), "http forward", err))
		logTurn("http", http.StatusBadGateway, hasPrev, "", start, k.store)
		return
	}
	defer resp.Body.Close()
	if task == nil {
		finishWrites, err := interruptCancelledWrites(w, r.Context())
		if err != nil {
			writeCancellationError(w, err)
			return
		}
		defer finishWrites()
	}
	if task != nil && task.state != nil {
		if err := task.state.extend(k.work, retainedHeaderBytes(resp.Header)); err != nil {
			writeAPIError(w, workingError(err))
			return
		}
	} else {
		headers, err := k.work.state(retainedHeaderBytes(resp.Header), nil)
		if err != nil {
			writeAPIError(w, workingError(err))
			return
		}
		defer headers.Close()
	}
	copyHeader(w.Header(), resp.Header)
	w.Header().Del("Content-Length")
	var c collector
	defer c.release(k.work)
	id := ""
	prepare := func() (*responseAcceptance, error) {
		if task != nil {
			rid := c.responseID()
			task.mu.Lock()
			if rid != "" && task.upstreamID != "" && task.upstreamID != rid {
				task.mu.Unlock()
				return nil, errUpstreamData
			}
			if rid != "" && task.upstreamID == "" {
				if err := task.state.extend(k.work, int64(len(rid))); err != nil {
					task.mu.Unlock()
					return nil, err
				}
				task.upstreamID = rid
			}
			task.mu.Unlock()
		}
		if rid, output, ok := c.final(); ok && t.store && id == "" {
			if task != nil {
				if !task.stream || c.status != "completed" {
					return &responseAcceptance{}, nil
				}
				rid = task.id
			}
			if err := r.Context().Err(); err != nil {
				return nil, err
			}
			pending, err := k.prepareAcceptDisk(r.Context(), t, publicID(rid), owner, output, "", false)
			if err != nil {
				logNotRetained(err)
				return &responseAcceptance{}, nil
			}
			return &responseAcceptance{close: pending.close, commit: func() (bool, error) {
				if _, err := pending.commit(r.Context()); err != nil {
					logNotRetained(err)
					return false, nil
				}
				id = "stored" // Only a success sentinel; the owned alias is in storage.
				return true, nil
			}}, nil
		}
		return nil, nil
	}
	if resp.StatusCode == http.StatusOK && strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
		w.WriteHeader(resp.StatusCode)
		rc := http.NewResponseController(w)
		var wire []byte
		var parsed map[string]json.RawMessage
		var release func()
		delivered := false // The terminal frame reached the client (or the task).
		err = readSSEOwned(resp.Body, func(frame []byte) error {
			if release != nil {
				defer release()
				defer func() { wire, parsed, release = nil, nil, nil }()
			}
			if task != nil {
				if wire == nil {
					return nil
				}
				err := task.record(r.Context(), wire, parsed)
				delivered = delivered || (err == nil && c.done)
				return err
			}
			if err := writeSSEFrame(w, frame, wire); err != nil {
				return err
			}
			err := rc.Flush()
			delivered = delivered || (err == nil && c.done)
			return err
		}, func(data *responseBuffer, suspend suspendResponseFrame) error {
			var e error
			wire, release, parsed, e = k.responseFrameOwned(r.Context(), &c, data, true, t.store, prepare, suspend)
			if e == nil && task != nil && task.nativeParent && parsed == nil {
				// Native parents keep upstream bytes, never a partial local chain.
				wire = data.data
			}
			return e
		}, k, k.work)
		if err == nil && !c.done {
			err = io.ErrUnexpectedEOF
		}
		if err != nil {
			log.Printf("http relay ended early: %v", err)
			// D9: a stored turn whose terminal was not relayed still ends in an explicit error event.
			if r.Context().Err() == nil && !delivered {
				event := failureEvent(c.responseID(), responseError(err), c.nextSeq())
				payload, _ := json.Marshal(event)
				_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event["type"], payload)
				_ = rc.Flush()
			}
		}
	} else {
		// Unexpected HTTP replies to background streams are opaque, not checkpoint documents.
		if resp.StatusCode != http.StatusOK || task != nil {
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
			return
		}
		body, err := readResponseBody(resp.Body, k.work)
		if err != nil {
			writeAPIError(w, responseError(err))
			return
		}
		defer body.close()
		wire, release, _, err := k.responseFrameOwned(r.Context(), &c, body, false, t.store, prepare, nil)
		if err != nil {
			writeAPIError(w, responseError(err))
			return
		}
		defer release()
		w.WriteHeader(resp.StatusCode)
		_, _ = w.Write(wire)
	}
	logTurn("http", resp.StatusCode, hasPrev, id, start, k.store)
}

var hopHeaders = []string{"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade"}

func copyHeader(dst, src http.Header) {
	clean := src.Clone()
	for _, name := range strings.Split(clean.Get("Connection"), ",") {
		if name = strings.TrimSpace(name); name != "" {
			clean.Del(name)
		}
	}
	for _, name := range hopHeaders {
		clean.Del(name)
	}
	for name, values := range clean {
		dst[name] = values
	}
}

func logTurn(transport string, status int, hasPrev bool, storedID string, start time.Time, s *store) {
	entries, size := s.stats()
	log.Printf("transport=%s status=%d previous=%t stored=%t duration=%s store_entries=%d store_bytes=%d",
		transport, status, hasPrev, storedID != "", time.Since(start).Round(time.Millisecond), entries, size)
}

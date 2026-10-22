package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// wsFakeUpstream speaks the Responses WebSocket protocol: each response.create
// is answered with a created/output/completed sequence; any frame carrying a
// previous_response_id is rejected, proving expanded turns reach it whole.
type wsFakeUpstream struct {
	mu     sync.Mutex
	frames []string
	n      int
}

func (f *wsFakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isUpgradeRequest(r) {
		f.serveHTTP(w, r)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	for {
		typ, raw, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		var doc struct {
			Type     string          `json:"type"`
			Prev     string          `json:"previous_response_id"`
			Generate *bool           `json:"generate"`
			Input    json.RawMessage `json:"input"`
			Model    string          `json:"model"`
		}
		_ = json.Unmarshal(raw, &doc)
		if typ != websocket.MessageText || doc.Type != "response.create" {
			// Echo untouched, like a control or out-of-band frame.
			_ = conn.Write(r.Context(), typ, raw)
			continue
		}
		f.mu.Lock()
		f.frames = append(f.frames, string(raw))
		f.n++
		n := f.n
		f.mu.Unlock()
		if doc.Generate != nil && !*doc.Generate {
			id := fmt.Sprintf("pre_%d", n)
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(
				`{"type":"response.created","response":{"id":"%s","object":"response","status":"in_progress","output":[]}}`, id)))
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(
				`{"type":"response.completed","response":{"id":"%s","object":"response","status":"completed","store":false,"output":[]}}`, id)))
			continue
		}
		if doc.Prev != "" {
			_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(
				`{"type":"error","status":400,"error":{"type":"invalid_request_error","code":"previous_response_not_found","message":"stateless upstream"}}`)))
			continue
		}
		var items []json.RawMessage
		_ = json.Unmarshal(doc.Input, &items)
		id := fmt.Sprintf("wsid_%d", n)
		reasoning := fmt.Sprintf(`{"type":"reasoning","id":"rs_%s","encrypted_content":"opaque-%s","summary":[]}`, id, id)
		message := fmt.Sprintf(`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"seen %d"}]}`, len(items))
		response := fmt.Sprintf(`{"id":"%s","object":"response","status":"completed","store":false,"output":[%s,%s]}`, id, reasoning, message)
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(
			`{"type":"response.created","response":{"id":"%s","object":"response","status":"in_progress","output":[]}}`, id)))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(
			`{"type":"response.output_item.done","output_index":0,"item":%s}`, reasoning)))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(
			`{"type":"response.output_item.done","output_index":1,"item":%s}`, message)))
		_ = conn.Write(r.Context(), websocket.MessageText, []byte(fmt.Sprintf(
			`{"type":"response.completed","response":%s}`, response)))
	}
}

// serveHTTP answers plain POSTs with the same output as the WS path, so HTTP
// and WS tests share one fake.
func (f *wsFakeUpstream) serveHTTP(w http.ResponseWriter, r *http.Request) {
	var doc map[string]json.RawMessage
	_ = json.NewDecoder(r.Body).Decode(&doc)
	if doc["previous_response_id"] != nil {
		http.Error(w, `{"error":{"message":"stateless upstream"}}`, http.StatusBadRequest)
		return
	}
	var input []json.RawMessage
	_ = json.Unmarshal(doc["input"], &input)
	f.mu.Lock()
	f.n++
	id := fmt.Sprintf("wsid_%d", f.n)
	f.mu.Unlock()
	reasoning := `{"type":"reasoning","id":"rs_` + id + `","encrypted_content":"opaque-` + id + `","summary":[]}`
	message := fmt.Sprintf(`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"seen %d"}]}`, len(input))
	response := `{"id":"` + id + `","object":"response","status":"completed","store":false,"output":[` + reasoning + `,` + message + `]}`
	var stream bool
	_ = json.Unmarshal(doc["stream"], &stream)
	if !stream {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(response))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprintf(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"%s\",\"status\":\"in_progress\",\"output\":[]}}\n\n", id)
	fmt.Fprintf(w, "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":%s}\n\n", reasoning)
	fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", response)
	_, _ = w.Write([]byte("data: [DONE]\n\n"))
}

func (f *wsFakeUpstream) last() []json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	var doc map[string]json.RawMessage
	_ = json.Unmarshal([]byte(f.frames[len(f.frames)-1]), &doc)
	var items []json.RawMessage
	_ = json.Unmarshal(doc["input"], &items)
	return items
}

func (f *wsFakeUpstream) frame(i int) map[string]json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	var doc map[string]json.RawMessage
	_ = json.Unmarshal([]byte(f.frames[i]), &doc)
	return doc
}

func newWSNativeKeeper(t *testing.T, opts ...func(*config)) (*wsFakeUpstream, *httptest.Server, *keeper) {
	t.Helper()
	up := &wsFakeUpstream{}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)
	u, _ := url.Parse(upSrv.URL)
	_, srv, k := newKeeperFixture(t, false, append(opts, func(c *config) { c.upstream = u })...)
	return up, srv, k
}

// A no-parent WS turn is recorded: the id is rewritten to a continuable resp_
// id, store flips true, and the upstream saw the request with no parent field.
func TestWSNativeFirstTurnRecorded(t *testing.T) {
	up, srv, k := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`)
	id := responseID(ev)
	if !strings.HasPrefix(id, "resp_wsid_1") {
		t.Fatalf("id not rewritten: %v", ev)
	}
	if resp, _ := ev["response"].(map[string]any); resp["store"] != true {
		t.Fatalf("recorded turn must advertise store:true: %v", ev)
	}
	if doc := up.frame(0); doc["previous_response_id"] != nil {
		t.Fatalf("parent leaked: %v", doc)
	}
	if n, _ := k.store.stats(); n != 1 {
		t.Fatalf("store entries = %d", n)
	}
}

// A recorded WS turn continues on the same socket and on a fresh socket: the
// expanded history reaches upstream, with assistant/reasoning output included.
func TestWSNativeContinuationExpandsAcrossSockets(t *testing.T) {
	up, srv, _ := newWSNativeKeeper(t)
	c1 := dialWS(t, srv, "Bearer a")
	id1 := responseID(wsCreate(t, c1, `{"type":"response.create","model":"m","input":"A"}`))
	id2 := responseID(wsCreate(t, c1, `{"type":"response.create","model":"m","input":"B","previous_response_id":"`+id1+`"}`))
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B" || id2 == "" {
		t.Fatalf("same-socket input = %s id2=%q", got, id2)
	}
	c1.Close(websocket.StatusNormalClosure, "")

	// Chain stays owned across three turns on a NEW socket.
	c2 := dialWS(t, srv, "Bearer a")
	id3 := responseID(wsCreate(t, c2, `{"type":"response.create","model":"m","input":"C","previous_response_id":"`+id2+`"}`))
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B,reasoning,assistant,user:C" || id3 == "" {
		t.Fatalf("new-socket input = %s id3=%q", got, id3)
	}
}

// A parent the keeper never recorded (native upstream id) stays downstream-
// native: byte-for-byte frame, unrewritten id, nothing stored for that turn.
func TestWSNativeUnknownParentPassthrough(t *testing.T) {
	up, srv, k := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	id1 := responseID(wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`))
	before, _ := k.store.stats()
	// A foreign continuation id that is not the keeper's must reach the
	// downstream verbatim — it answers with its own parent-resolution error.
	bad := `{"type":"response.create","model":"m","input":"B","previous_response_id":"resp_upstream_owned"}`
	ev := wsCreate(t, c, bad)
	if errObj, _ := ev["error"].(map[string]any); ev["type"] != "error" || errObj["code"] != "previous_response_not_found" {
		t.Fatalf("expected downstream not-found error, got %v", ev)
	}
	if got := string(mustRaw(t, up, 1)); got != bad {
		t.Fatalf("native frame changed:\n got: %s\nwant: %s", got, bad)
	}
	after, _ := k.store.stats()
	if after != before || before != 1 {
		t.Fatalf("native turn recorded: %d -> %d", before, after)
	}
	_ = id1
}

func mustRaw(t *testing.T, up *wsFakeUpstream, i int) []byte {
	t.Helper()
	up.mu.Lock()
	defer up.mu.Unlock()
	return []byte(up.frames[i])
}

// An owned-but-expired/foreign keeper id fails closed on WS like HTTP does.
func TestWSNativeExpiredParentFailsClosed(t *testing.T) {
	up, srv, k := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	id1 := responseID(wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`))
	k.store.mu.Lock()
	k.store.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
	k.store.mu.Unlock()
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"B","previous_response_id":"`+id1+`"}`)
	if code, ok := failureCode(ev); ev["type"] != "error" || !ok || code != "previous_response_not_found" {
		t.Fatalf("expired parent must fail closed: %v", ev)
	}
	// The socket stays usable after a fail-closed turn.
	ev = wsCreate(t, c, `{"type":"response.create","model":"m","input":"B"}`)
	if ev["type"] != "response.completed" {
		t.Fatalf("socket unusable after fail-closed turn: %v", ev)
	}
	_ = up
}

// generate:false prewarm frames carry no parent; they forward untouched and
// their (empty) turn is not recorded as continuable output.
func TestWSNativePrewarmPassthrough(t *testing.T) {
	up, srv, k := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","generate":false,"input":[]}`)
	if resp, _ := ev["response"].(map[string]any); ev["type"] != "response.completed" {
		t.Fatalf("prewarm: %v %v", ev, resp)
	}
	var g any
	if doc := up.frame(0); json.Unmarshal(doc["generate"], &g) != nil || g != false {
		t.Fatalf("generate flag altered: %v", doc)
	}
	// Empty output and store:false → nothing continuable recorded.
	if n, _ := k.store.stats(); n != 0 {
		t.Fatalf("prewarm recorded: %d", n)
	}
}

// A native upgrade refused downstream keeps the upstream status and body.
func TestWSNativeNon101Preserved(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Probe", "kept")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"denied"}}`))
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	ctx, cancel := timeoutCtx(5 * time.Second)
	defer cancel()
	_, resp, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/responses", &websocket.DialOptions{})
	if err == nil {
		t.Fatal("dial unexpectedly succeeded")
	}
	if resp == nil || resp.StatusCode != 403 || resp.Header.Get("X-Probe") != "kept" {
		t.Fatalf("non-101 not preserved: %v %+v", err, resp)
	}
}

// A native WS turn's response frames reach the client untouched when the turn
// is downstream-owned (native parent), including no id rewriting.
func TestWSNativeNativeTurnFramesUntouched(t *testing.T) {
	up, srv, _ := newWSNativeKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	// Native parent: frame forwarded verbatim; upstream answers its own error.
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"B","previous_response_id":"resp_native_upstream"}`)
	if errObj, _ := ev["error"].(map[string]any); ev["type"] != "error" {
		t.Fatalf("expected upstream error frame, got %v", ev)
	} else if errObj["code"] != "previous_response_not_found" {
		t.Fatalf("upstream error rewritten: %v", ev)
	}
	if doc := up.frame(0); string(doc["previous_response_id"]) != `"resp_native_upstream"` {
		t.Fatalf("native parent changed: %v", doc)
	}
	_ = up
}

// Closing the client mid-turn cancels the upstream read path: no goroutine
// leak, no panic, keeper keeps serving new sockets.
func TestWSNativeClientCloseCancels(t *testing.T) {
	upSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		// Read the create then hang until the peer goes away.
		for {
			if _, _, err := conn.Read(r.Context()); err != nil {
				return
			}
		}
	}))
	defer upSrv.Close()
	u, _ := url.Parse(upSrv.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	conn := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"A"}`)); err != nil {
		t.Fatal(err)
	}
	conn.CloseNow()
	// A fresh socket still works.
	c2 := dialWS(t, srv, "Bearer a")
	_ = c2.Close(websocket.StatusNormalClosure, "")
}

// Storage failure still serves the WS turn: the response relays with the
// upstream store value so the client never continues from an unrecorded id.
func TestWSNativeStorageFailureServesTurn(t *testing.T) {
	up := &wsFakeUpstream{}
	upSrv := httptest.NewServer(up)
	defer upSrv.Close()
	u, _ := url.Parse(upSrv.URL)
	k := newKeeper(config{upstream: u, ttl: time.Minute, maxStoreBytes: 1, maxRequestBytes: 1 << 20,
		storeDir: t.TempDir(), storeMetaBytes: 1 << 20, storePinBytes: 1, storeCompactBytes: 4 << 20})
	defer k.store.close()
	srv := httptest.NewServer(k)
	defer srv.Close()
	c := dialWS(t, srv, "Bearer a")
	assertServedUncached(t, wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`))
}

// A frame beyond the request budget is rejected like in local mode: the turn
// gets an explicit working_set_too_large error frame, then the socket closes
// with StatusMessageTooBig rather than serving a partial read.
func TestWSNativeOversizeFrame(t *testing.T) {
	_, srv, _ := newWSNativeKeeper(t, func(c *config) { c.maxRequestBytes = 128 })
	c := dialWS(t, srv, "Bearer a")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	big := `{"type":"response.create","model":"m","input":"` + strings.Repeat("x", 1024) + `"}`
	if err := c.Write(ctx, websocket.MessageText, []byte(big)); err != nil {
		t.Fatal(err)
	}
	var ev map[string]any
	_, data, err := c.Read(ctx)
	if err != nil || json.Unmarshal(data, &ev) != nil || ev["type"] != "error" {
		t.Fatalf("oversize turn: err=%v frame=%s", err, data)
	}
	errObj, _ := ev["error"].(map[string]any)
	if errObj["code"] != "working_set_too_large" {
		t.Fatalf("oversize code: %v", ev)
	}
	_, _, err = c.Read(ctx)
	var ce websocket.CloseError
	if err == nil || !errors.As(err, &ce) || ce.Code != websocket.StatusMessageTooBig {
		t.Fatalf("oversize close: err=%v", err)
	}
}

// HTTP: a no-parent turn through the standard dispatcher is recorded, its id
// rewritten, and the chain expands across three turns under one owner.
func TestNativeHTTPChainRecordedAndExpanded(t *testing.T) {
	up, srv, k := newWSNativeKeeper(t)
	auth := "Bearer a"
	_, body, _ := post(t, srv, auth, `{"model":"m","input":"A","stream":true}`)
	id1 := completedID(t, body)
	if !strings.HasPrefix(id1, "resp_wsid_") && !strings.HasPrefix(id1, "resp_") {
		t.Fatalf("id not published as response id: %s", id1)
	}
	_, body, _ = post(t, srv, auth, `{"model":"m","input":"B","stream":true,"previous_response_id":"`+id1+`"}`)
	id2 := completedID(t, body)
	_, body, _ = post(t, srv, auth, `{"model":"m","input":"C","stream":true,"previous_response_id":"`+id2+`"}`)
	completedID(t, body)
	// wsFakeUpstream rejects previous_response_id; reaching turn 3 proves every
	// expansion removed the reference and replayed the full stored history.
	_ = up
	if n, _ := k.store.stats(); n != 3 {
		t.Fatalf("chain entries = %d", n)
	}
}

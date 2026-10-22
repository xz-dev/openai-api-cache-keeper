package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// fakeUpstream is a stateless Responses server. It rejects previous_response_id,
// records every input it receives, and answers with a reasoning item plus a
// message naming how many input items it saw.
type fakeUpstream struct {
	mu     sync.Mutex
	inputs [][]json.RawMessage
	n      int
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var doc map[string]json.RawMessage
	_ = json.NewDecoder(r.Body).Decode(&doc)
	var model string
	_ = json.Unmarshal(doc["model"], &model)
	if strings.HasPrefix(model, "status-") {
		var code int
		fmt.Sscanf(model, "status-%d", &code)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(code)
		fmt.Fprintf(w, `{"error":{"type":"invalid_request_error","code":"upstream_%d","message":"full upstream detail that must not be truncated by the keeper"}}`, code)
		return
	}
	if doc["previous_response_id"] != nil {
		http.Error(w, `{"error":{"message":"stateless upstream"}}`, http.StatusBadRequest)
		return
	}
	var input []json.RawMessage
	_ = json.Unmarshal(doc["input"], &input)
	f.mu.Lock()
	f.inputs = append(f.inputs, input)
	f.n++
	id := fmt.Sprintf("interaction_%d", f.n)
	if model == "anthropic" {
		id = fmt.Sprintf("msg_%d", f.n)
	}
	f.mu.Unlock()
	reasoning := `{"type":"reasoning","id":"rs_` + id + `","encrypted_content":"opaque-` + id + `","summary":[]}`
	message := fmt.Sprintf(`{"type":"message","role":"assistant","content":[{"type":"output_text","text":"seen %d"}]}`, len(input))
	// Like the production stateless upstream: it reports store:false regardless of the request.
	response := `{"id":"` + id + `","object":"response","status":"completed","store":false,"output":[` + reasoning + `,` + message + `]}`
	var stream bool
	_ = json.Unmarshal(doc["stream"], &stream)
	if !stream {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, response)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	fmt.Fprintf(w, "event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"%s\",\"status\":\"in_progress\",\"output\":[]}}\n\n", id)
	fmt.Fprintf(w, "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":%s}\n\n", reasoning)
	fmt.Fprintf(w, "event: response.output_item.done\ndata: {\"type\":\"response.output_item.done\",\"output_index\":1,\"item\":%s}\n\n", message)
	fmt.Fprintf(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":%s}\n\n", response)
	io.WriteString(w, "data: [DONE]\n\n")
}

func (f *fakeUpstream) last() []json.RawMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.inputs[len(f.inputs)-1]
}

func newTestKeeper(t *testing.T, opts ...func(*config)) (*fakeUpstream, *httptest.Server, *keeper) {
	return newKeeperFixture(t, false, opts...)
}

func newKeeperFixture(t *testing.T, local bool, opts ...func(*config)) (*fakeUpstream, *httptest.Server, *keeper) {
	t.Helper()
	up := &fakeUpstream{}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)
	u, _ := url.Parse(upSrv.URL)
	cfg := config{upstream: u, ttl: time.Minute, maxStoreBytes: 1 << 20, maxRequestBytes: 1 << 20,
		storeDir: t.TempDir(), storeMetaBytes: 1 << 20, storePinBytes: 1 << 20, storeCompactBytes: 4 << 20}
	for _, opt := range opts {
		opt(&cfg)
	}
	k := newKeeper(cfg)
	var handler http.Handler = k
	if local {
		handler = http.HandlerFunc(k.serveLocalTurn)
	}
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = k.store.close() })
	t.Cleanup(func() {
		k.cacheMu.Lock()
		tasks := make([]*responseTask, 0, len(k.tasks))
		for _, task := range k.tasks {
			tasks = append(tasks, task)
		}
		k.cacheMu.Unlock()
		for _, task := range tasks {
			task.expire()
		}
		for _, task := range tasks {
			select {
			case <-task.finished:
			case <-time.After(workWait):
				t.Error("background task did not stop")
			}
		}
	})
	return up, srv, k
}

func post(t *testing.T, srv *httptest.Server, auth, body string) (int, string, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", auth)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw), resp.Header
}

func completedID(t *testing.T, sse string) string {
	t.Helper()
	var id string
	_ = readSSE(strings.NewReader(sse), nil, func(data []byte) error {
		var ev struct {
			Type     string `json:"type"`
			Response struct {
				ID string `json:"id"`
			} `json:"response"`
		}
		if json.Unmarshal(data, &ev) == nil && ev.Type == "response.completed" {
			id = ev.Response.ID
		}
		return nil
	})
	if id == "" {
		t.Fatalf("no response.completed in %q", sse)
	}
	return id
}

func kinds(items []json.RawMessage) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		var v struct {
			Type    string          `json:"type"`
			Role    string          `json:"role"`
			Content json.RawMessage `json:"content"`
		}
		_ = json.Unmarshal(item, &v)
		var text string
		if json.Unmarshal(v.Content, &text) != nil {
			text = ""
		}
		switch {
		case v.Type == "reasoning":
			out = append(out, "reasoning")
		case v.Role == "user":
			out = append(out, "user:"+text)
		default:
			out = append(out, v.Role)
		}
	}
	return out
}

func TestHTTPDeltaContinuationAndRollback(t *testing.T) {
	up, srv, _ := newLocalTestKeeper(t)
	auth := "Bearer a"
	status, body, _ := post(t, srv, auth, `{"model":"m","input":"A","stream":true}`)
	if status != 200 {
		t.Fatalf("turn1 %d %s", status, body)
	}
	id1 := completedID(t, body)

	status, body, _ = post(t, srv, auth, `{"model":"m","input":"B","stream":true,"previous_response_id":"`+id1+`"}`)
	if status != 200 {
		t.Fatalf("turn2 %d %s", status, body)
	}
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B" {
		t.Fatalf("turn2 upstream input = %s", got)
	}
	if !strings.Contains(string(up.last()[1]), "opaque-interaction_1") {
		t.Fatalf("reasoning item not replayed verbatim: %s", up.last()[1])
	}

	// Rollback: continue from turn 1 again, excluding turn 2.
	status, _, _ = post(t, srv, auth, `{"model":"m","input":"C","stream":false,"previous_response_id":"`+id1+`"}`)
	if status != 200 {
		t.Fatalf("rollback %d", status)
	}
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:C" {
		t.Fatalf("rollback upstream input = %s", got)
	}
}

func TestMessageStyleIDsArePublishedAsResponseIDs(t *testing.T) {
	up, srv, _ := newLocalTestKeeper(t)
	_, body, _ := post(t, srv, "Bearer a", `{"model":"anthropic","input":"A","stream":true}`)
	id := completedID(t, body)
	if id != "resp_msg_1" || strings.Contains(body, `"id":"msg_1"`) {
		t.Fatalf("stream id = %q body=%s", id, body)
	}
	status, body, _ := post(t, srv, "Bearer a", `{"model":"anthropic","input":"B","stream":false,"previous_response_id":"resp_msg_1"}`)
	if status != 200 || !strings.Contains(body, `"id":"resp_msg_2"`) {
		t.Fatalf("json continuation %d %s", status, body)
	}
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B" {
		t.Fatalf("upstream input = %s", got)
	}
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"anthropic","input":"C","previous_response_id":"resp_msg_2"}`)
	if responseID(ev) != "resp_msg_3" {
		t.Fatalf("ws id = %v", ev)
	}
}

func TestHTTPUnknownAndForeignIDs(t *testing.T) {
	_, srv, _ := newLocalTestKeeper(t)
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":true}`)
	completedID(t, body)
	// The local handler still fails closed on an id it cannot expand (an
	// id it never saw, or one that expired). The dispatcher's native branch
	// for never-seen ids is covered by TestNativeUpstreamIDRemainsNative.
	for _, tc := range []struct{ auth, id string }{{"Bearer a", "missing"}, {"Bearer b", "missing"}} {
		status, body, _ = post(t, srv, tc.auth, `{"model":"m","input":"B","previous_response_id":"`+tc.id+`"}`)
		if status != 400 || !strings.Contains(body, `"code":"previous_response_not_found"`) {
			t.Fatalf("%s/%s: %d %s", tc.auth, tc.id, status, body)
		}
	}
}

func TestHTTPStoreFalseIsNotKept(t *testing.T) {
	_, srv, k := newLocalTestKeeper(t)
	_, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":true,"store":false}`)
	completedID(t, body)
	if n, _ := k.store.stats(); n != 0 {
		t.Fatalf("store:false kept %d entries", n)
	}
}

func TestUpstreamErrorsPassThroughVerbatim(t *testing.T) {
	_, srv, _ := newLocalTestKeeper(t)
	for _, code := range []int{400, 404, 413, 429, 500} {
		status, body, header := post(t, srv, "Bearer a", fmt.Sprintf(`{"model":"status-%d","input":"x","stream":true}`, code))
		if status != code || !strings.Contains(body, "must not be truncated by the keeper") || header.Get("Retry-After") != "7" {
			t.Fatalf("%d: got %d %q retry-after=%q", code, status, body, header.Get("Retry-After"))
		}
	}
}

func dialWS(t *testing.T, srv *httptest.Server, auth string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/responses", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {auth}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

// wsCreate sends one response.create and returns the terminal frame.
func wsCreate(t *testing.T, conn *websocket.Conn, frame string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var ev map[string]any
		if err := json.Unmarshal(data, &ev); err != nil {
			t.Fatalf("non-JSON frame %q", data)
		}
		switch ev["type"] {
		case "response.completed", "response.incomplete", "response.failed", "error":
			return ev
		}
	}
}

// assertServedUncached checks the fail-open storage contract: the turn still
// completes, but is not advertised as stored (so clients will not continue it).
func assertServedUncached(t *testing.T, ev map[string]any) {
	t.Helper()
	resp, _ := ev["response"].(map[string]any)
	if ev["type"] != "response.completed" || resp["store"] == true {
		t.Fatalf("storage failure must serve the turn uncached: %v", ev)
	}
}

// terminalStored reports whether the terminal response in a JSON or SSE body
// is advertised as stored; the terminal value is authoritative because earlier
// stream events precede the storage decision.
func terminalStored(t *testing.T, body string) bool {
	t.Helper()
	var stored *bool
	check := func(raw []byte) {
		var ev struct {
			Type     string          `json:"type"`
			Store    *bool           `json:"store"`
			Response json.RawMessage `json:"response"`
		}
		if json.Unmarshal(raw, &ev) != nil {
			return
		}
		if ev.Type == "response.completed" {
			var resp struct {
				Store *bool `json:"store"`
			}
			_ = json.Unmarshal(ev.Response, &resp)
			v := resp.Store != nil && *resp.Store
			stored = &v
		} else if ev.Type == "" {
			v := ev.Store != nil && *ev.Store
			stored = &v
		}
	}
	if strings.HasPrefix(strings.TrimSpace(body), "{") {
		check([]byte(body))
	} else {
		_ = readSSE(strings.NewReader(body), nil, func(data []byte) error { check(data); return nil })
	}
	if stored == nil {
		t.Fatalf("no terminal response in %q", body)
	}
	return *stored
}

// failureCode returns the error code of a request error or a response.failed
// terminal event; ok is false for any other event.
func failureCode(ev map[string]any) (string, bool) {
	obj := ev
	switch ev["type"] {
	case "error":
	case "response.failed":
		obj, _ = ev["response"].(map[string]any)
	default:
		return "", false
	}
	errObj, _ := obj["error"].(map[string]any)
	code, _ := errObj["code"].(string)
	return code, true
}

func responseID(ev map[string]any) string {
	resp, _ := ev["response"].(map[string]any)
	id, _ := resp["id"].(string)
	return id
}

func TestWSSameSocketNewSocketAndStoreFalse(t *testing.T) {
	up, srv, _ := newLocalTestKeeper(t)
	c1 := dialWS(t, srv, "Bearer a")
	id1 := responseID(wsCreate(t, c1, `{"type":"response.create","model":"m","input":"A"}`))
	id2 := responseID(wsCreate(t, c1, `{"type":"response.create","model":"m","input":"B","previous_response_id":"`+id1+`"}`))
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B" || id2 == "" {
		t.Fatalf("same-socket input = %s", got)
	}

	// Reconnect: a new socket continues from the stored turn.
	c1.Close(websocket.StatusNormalClosure, "")
	c2 := dialWS(t, srv, "Bearer a")
	wsCreate(t, c2, `{"type":"response.create","model":"m","input":"C","previous_response_id":"`+id2+`"}`)
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B,reasoning,assistant,user:C" {
		t.Fatalf("new-socket input = %s", got)
	}

	// store:false works within the socket only.
	id3 := responseID(wsCreate(t, c2, `{"type":"response.create","model":"m","input":"D","store":false}`))
	ev := wsCreate(t, c2, `{"type":"response.create","model":"m","input":"E","store":false,"previous_response_id":"`+id3+`"}`)
	if ev["type"] != "response.completed" {
		t.Fatalf("store:false same-socket continuation failed: %v", ev)
	}
	c3 := dialWS(t, srv, "Bearer a")
	ev = wsCreate(t, c3, `{"type":"response.create","model":"m","input":"F","previous_response_id":"`+id3+`"}`)
	errObj, _ := ev["error"].(map[string]any)
	if ev["type"] != "error" || errObj["code"] != "previous_response_not_found" {
		t.Fatalf("store:false id leaked across sockets: %v", ev)
	}
}

func TestWSUpstreamErrorFrame(t *testing.T) {
	_, srv, _ := newLocalTestKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"status-429","input":"x"}`)
	errObj, _ := ev["error"].(map[string]any)
	if ev["type"] != "error" || ev["status"] != float64(429) || errObj["code"] != "upstream_429" {
		t.Fatalf("got %v", ev)
	}
}

func TestStoreTTLAndByteBudget(t *testing.T) {
	s, err := newStore(t.TempDir(), time.Minute, 10, 1<<20, 1<<20, 4<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.close() })
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }
	owner := [32]byte{1}
	s.put("a", owner, "", []json.RawMessage{json.RawMessage(`"1234"`)}) // 6 bytes
	s.put("b", owner, "", []json.RawMessage{json.RawMessage(`"1236"`)}) // 6 more: 12 > 10 evicts a
	if _, ok := s.get("a", owner); ok {
		t.Fatal("a should be evicted by the byte budget")
	}
	if _, ok := s.get("b", owner); !ok {
		t.Fatal("b should be kept")
	}
	if _, err := s.put("huge", owner, "", []json.RawMessage{json.RawMessage(`"this is far too large"`)}); err == nil {
		t.Fatal("entry larger than the budget must be refused")
	}
	if _, ok := s.get("huge", owner); ok {
		t.Fatal("refused entry must not be readable")
	}
	now = now.Add(time.Minute)
	if _, ok := s.get("b", owner); ok {
		t.Fatal("b should expire after the TTL")
	}
	if n, size := s.stats(); n != 0 || size != 0 {
		t.Fatalf("stats after expiry = %d/%d", n, size)
	}
}

func TestReadSSEMultilineAndDone(t *testing.T) {
	var got []string
	_ = readSSE(bufio.NewReader(strings.NewReader("data: {\"a\":\ndata: 1}\n\n: comment\n\ndata: [DONE]\n\n")), nil, func(d []byte) error {
		got = append(got, string(d))
		return nil
	})
	if len(got) != 1 || got[0] != "{\"a\":\n1}" {
		t.Fatalf("got %q", got)
	}
}

func storeOf(t *testing.T, payload string) any {
	t.Helper()
	var doc map[string]any
	if err := json.Unmarshal([]byte(payload), &doc); err != nil {
		t.Fatalf("payload %q: %v", payload, err)
	}
	if resp, ok := doc["response"].(map[string]any); ok {
		return resp["store"]
	}
	return doc["store"]
}

func completedStore(t *testing.T, sse string) any {
	t.Helper()
	var store any = "missing"
	_ = readSSE(strings.NewReader(sse), nil, func(data []byte) error {
		if strings.Contains(string(data), `"type":"response.completed"`) {
			store = storeOf(t, string(data))
		}
		return nil
	})
	return store
}

// The keeper is the store: a turn it keeps is declared store:true to the client
// even though the stateless upstream reported store:false; a store:false
// request keeps the upstream value.
func TestStoredTurnsAreDeclaredStored(t *testing.T) {
	_, srv, _ := newLocalTestKeeper(t)
	_, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":true}`)
	if got := completedStore(t, body); got != true {
		t.Fatalf("sse store = %v", got)
	}
	_, body, _ = post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":false}`)
	if got := storeOf(t, body); got != true {
		t.Fatalf("json store = %v", got)
	}
	_, body, _ = post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":true,"store":false}`)
	if got := completedStore(t, body); got != false {
		t.Fatalf("store:false sse store = %v", got)
	}
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"m","input":"A"}`)
	if resp, _ := ev["response"].(map[string]any); resp["store"] != true {
		t.Fatalf("ws store = %v", ev)
	}
	ev = wsCreate(t, c, `{"type":"response.create","model":"m","input":"B","store":false}`)
	if resp, _ := ev["response"].(map[string]any); resp["store"] != false {
		t.Fatalf("ws store:false = %v", ev)
	}
}

// IGNORE_CLIENT_STORE_FALSE: store:false turns (Codex) are kept, continuable on a
// new socket or over HTTP, and declared stored to the client.
func TestIgnoreClientStoreFalse(t *testing.T) {
	up, srv, k := newLocalTestKeeper(t, func(c *config) { c.ignoreClientStoreFalse = true })
	_, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":true,"store":false}`)
	id1 := completedID(t, body)
	if got := completedStore(t, body); got != true {
		t.Fatalf("http store = %v", got)
	}
	if n, _ := k.store.stats(); n != 1 {
		t.Fatalf("stored entries = %d", n)
	}
	c1 := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c1, `{"type":"response.create","model":"m","input":"B","store":false,"previous_response_id":"`+id1+`"}`)
	if resp, _ := ev["response"].(map[string]any); ev["type"] != "response.completed" || resp["store"] != true {
		t.Fatalf("ws continuation = %v", ev)
	}
	c1.Close(websocket.StatusNormalClosure, "")
	c2 := dialWS(t, srv, "Bearer a")
	ev = wsCreate(t, c2, `{"type":"response.create","model":"m","input":"C","store":false,"previous_response_id":"`+responseID(ev)+`"}`)
	if ev["type"] != "response.completed" {
		t.Fatalf("new-socket continuation = %v", ev)
	}
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B,reasoning,assistant,user:C" {
		t.Fatalf("new-socket input = %s", got)
	}
}

func TestLoadConfigIgnoreClientStoreFalse(t *testing.T) {
	t.Setenv("UPSTREAM_URL", "http://upstream:8080")
	for _, tc := range []struct {
		value string
		want  bool
		err   bool
	}{{"", false, false}, {"false", false, false}, {"true", true, false}, {"1", true, false}, {"yes", false, true}} {
		t.Setenv("IGNORE_CLIENT_STORE_FALSE", tc.value)
		cfg, err := loadConfig()
		if (err != nil) != tc.err || (err == nil && cfg.ignoreClientStoreFalse != tc.want) {
			t.Fatalf("%q: cfg=%v err=%v", tc.value, cfg.ignoreClientStoreFalse, err)
		}
	}
}

// Local-history unit tests exercise the adapter directly; native_test.go tests
// the standard-path dispatcher without a private path or routing dependency.
func newLocalTestKeeper(t *testing.T, opts ...func(*config)) (*fakeUpstream, *httptest.Server, *keeper) {
	return newKeeperFixture(t, true, opts...)
}

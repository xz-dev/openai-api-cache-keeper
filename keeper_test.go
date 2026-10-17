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
	response := `{"id":"` + id + `","object":"response","status":"completed","output":[` + reasoning + `,` + message + `]}`
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

func newTestKeeper(t *testing.T) (*fakeUpstream, *httptest.Server, *keeper) {
	t.Helper()
	up := &fakeUpstream{}
	upSrv := httptest.NewServer(up)
	t.Cleanup(upSrv.Close)
	u, _ := url.Parse(upSrv.URL)
	k := newKeeper(config{upstream: u, ttl: time.Minute, maxStoreBytes: 1 << 20, maxRequestBytes: 1 << 20})
	srv := httptest.NewServer(k)
	t.Cleanup(srv.Close)
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
	up, srv, _ := newTestKeeper(t)
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
	up, srv, _ := newTestKeeper(t)
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
	_, srv, _ := newTestKeeper(t)
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":true}`)
	id := completedID(t, body)
	for _, tc := range []struct{ auth, id string }{{"Bearer a", "missing"}, {"Bearer b", id}} {
		status, body, _ = post(t, srv, tc.auth, `{"model":"m","input":"B","previous_response_id":"`+tc.id+`"}`)
		if status != 400 || !strings.Contains(body, `"code":"previous_response_not_found"`) {
			t.Fatalf("%s/%s: %d %s", tc.auth, tc.id, status, body)
		}
	}
}

func TestHTTPStoreFalseIsNotKept(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	_, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"A","stream":true,"store":false}`)
	completedID(t, body)
	if n, _ := k.store.stats(); n != 0 {
		t.Fatalf("store:false kept %d entries", n)
	}
}

func TestUpstreamErrorsPassThroughVerbatim(t *testing.T) {
	_, srv, _ := newTestKeeper(t)
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
		if ev["type"] == "response.completed" || ev["type"] == "error" {
			return ev
		}
	}
}

func responseID(ev map[string]any) string {
	resp, _ := ev["response"].(map[string]any)
	id, _ := resp["id"].(string)
	return id
}

func TestWSSameSocketNewSocketAndStoreFalse(t *testing.T) {
	up, srv, _ := newTestKeeper(t)
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
	_, srv, _ := newTestKeeper(t)
	c := dialWS(t, srv, "Bearer a")
	ev := wsCreate(t, c, `{"type":"response.create","model":"status-429","input":"x"}`)
	errObj, _ := ev["error"].(map[string]any)
	if ev["type"] != "error" || ev["status"] != float64(429) || errObj["code"] != "upstream_429" {
		t.Fatalf("got %v", ev)
	}
}

func TestStoreTTLAndByteBudget(t *testing.T) {
	s := newStore(time.Minute, 10)
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }
	owner := [32]byte{1}
	s.put("a", owner, []json.RawMessage{json.RawMessage(`"1234"`)}) // 6 bytes
	s.put("b", owner, []json.RawMessage{json.RawMessage(`"1234"`)}) // 12 > 10: evicts a
	if _, ok := s.get("a", owner); ok {
		t.Fatal("a should be evicted by the byte budget")
	}
	if _, ok := s.get("b", owner); !ok {
		t.Fatal("b should be kept")
	}
	s.put("huge", owner, []json.RawMessage{json.RawMessage(`"this is far too large"`)})
	if _, ok := s.get("huge", owner); ok {
		t.Fatal("entry larger than the budget must not be stored")
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

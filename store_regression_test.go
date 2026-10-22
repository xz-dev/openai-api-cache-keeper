package main

import (
	"bytes"
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

	bolt "go.etcd.io/bbolt"
)

func TestReviewEvictedContentCanBeRestored(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	for i := 0; i < 100; i++ {
		text := fmt.Sprintf(`"A%d"`, i)
		mustReviewPut(t, s, "old", o, "", items(text))
		if err := dropTestResponse(s, "old"); err != nil {
			t.Fatal(err)
		}
		mustReviewPut(t, s, "new", o, "", items(text))
		roundtrip(t, s, "new", o, []string{text})
		if err := dropTestResponse(s, "new"); err != nil {
			t.Fatal(err)
		}
		if live, meta, _, nodes, aliases := inspectStore(t, s); live != 0 || meta != 0 || nodes != 0 || aliases != 0 {
			t.Fatalf("churn retained records: %d/%d/%d/%d", live, meta, nodes, aliases)
		}
	}
}
func TestReviewParentSurvivesAdmissionEviction(t *testing.T) {
	s := newTestStore(t, time.Hour, 8, 1<<20, 4, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "parent", o, "", items(`"A"`))
	if _, err := s.put("child", o, "parent", items(`"A"`, `"BBBB"`)); err != errStoreBudget {
		t.Fatalf("over-budget history err=%v", err)
	}
	roundtrip(t, s, "parent", o, []string{`"A"`})
	// Shared content belonged to an LRU victim, but survives in the admitted chain.
	mustReviewPut(t, s, "victim", o, "", items(`"B"`))
	mustReviewPut(t, s, "child", o, "parent", items(`"A"`, `"C"`))
	roundtrip(t, s, "child", o, []string{`"A"`, `"C"`})
	inspectStore(t, s)
}
func TestReviewMetadataAliasesBounded(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1024, 1<<19, 4<<20)
	o := [32]byte{1}
	for i := 0; i < 100; i++ {
		id := fmt.Sprint("independent-id-", i)
		mustReviewPut(t, s, id, o, "", items(`"A"`))
		roundtrip(t, s, id, o, []string{`"A"`})
		_, _, _, _, aliases := inspectStore(t, s)
		if int64(aliases)*respMetaOverhead > s.maxMeta {
			t.Fatal("alias metadata cap bypass")
		}
	}
}
func TestReviewMetadataRepeatedHashesBounded(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1024, 1<<19, 4<<20)
	o := [32]byte{1}
	full := make([]json.RawMessage, 10000)
	for i := range full {
		full[i] = json.RawMessage(`"A"`)
	}
	if _, err := s.put("many-items", o, "", full); err != errStoreBudget {
		t.Fatalf("repeated-item cap bypass: %v", err)
	}
	inspectStore(t, s)
	// Every admitted repeated occurrence must round-trip; no truncation to fit.
	for count := 1; count <= 8; count++ {
		if _, err := s.put("repeats", o, "", full[:count]); err == nil {
			got, ok := s.get("repeats", o)
			if !ok || len(got) != count {
				t.Fatalf("count %d lost", count)
			}
			for _, raw := range got {
				if string(raw) != `"A"` {
					t.Fatal("changed item")
				}
			}
		} else if err != errStoreBudget {
			t.Fatal(err)
		}
		inspectStore(t, s)
	}
}
func TestReviewPinRefusalPreservesExistingNode(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 15, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "registered", o, "", items(`"AAAAAAAA"`))
	pin, release, err := s.putPinned(o, "", "", items(`"AAAAAAAA"`))
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	n, rel, err := s.putPinned(o, "", "", items(`"AAAAAAAA"`))
	rel()
	if n != "" || err != errStoreBudget {
		t.Fatalf("expected alias pin refusal: %v", err)
	}
	roundtrip(t, s, "registered", o, []string{`"AAAAAAAA"`})
	got, ok := s.getPinned(pin, o)
	if !ok {
		t.Fatal("other socket pin stolen")
	}
	checkItems(t, got, []string{`"AAAAAAAA"`})
	inspectStore(t, s)
	release()
	release()
	inspectStore(t, s)
}
func TestReviewCompactionTriggerRuns(t *testing.T) {
	s := newTestStore(t, time.Minute, 256<<10, 1<<20, 1<<17, 32<<10)
	o := [32]byte{1}
	now := time.Unix(0, 0)
	s.now = func() time.Time { return now }
	raw, _ := json.Marshal(strings.Repeat("X", 128<<10))
	mustReviewPut(t, s, "large", o, "", []json.RawMessage{raw})
	before := s.diskUsage()
	now = now.Add(2 * time.Minute)
	mustReviewPut(t, s, "small", o, "", items(`"Z"`))
	roundtrip(t, s, "small", o, []string{`"Z"`})
	if after := s.diskUsage(); after >= before || after > 3*s.compactAt {
		t.Fatalf("file not reclaimed: before=%d after=%d trigger=%d", before, after, s.compactAt)
	}
	inspectStore(t, s)
}
func TestReviewStoreFalseThreeTurns(t *testing.T) {
	_, srv, _ := newLocalTestKeeper(t)
	c := dialWS(t, srv, "Bearer separate-owner-1")
	ev := wsCreate(t, c, `{"type":"response.create","input":"A","store":false}`)
	for _, input := range []string{"B", "C", "D"} {
		id := responseID(ev)
		if id == "" {
			t.Fatalf("missing id: %v", ev)
		}
		ev = wsCreate(t, c, fmt.Sprintf(`{"type":"response.create","input":%q,"store":false,"previous_response_id":%q}`, input, id))
		if ev["type"] != "response.completed" {
			t.Fatalf("same-socket turn %s failed: %v", input, ev)
		}
	}
}

// Store refusal is a cache miss, not a request failure: the turn is served,
// not advertised as stored, and its id is not continuable.
func TestReviewStoreRefusalServesUncachedHTTP(t *testing.T) {
	_, srv, _ := newLocalTestKeeper(t, func(c *config) { c.maxStoreBytes = 1 })
	for _, stream := range []bool{false, true} {
		status, body, _ := post(t, srv, "Bearer visible-refusal", fmt.Sprintf(`{"input":"A","stream":%v}`, stream))
		if status != 200 || strings.Contains(body, "history_storage_failed") || !strings.Contains(body, `"status":"completed"`) || terminalStored(t, body) {
			t.Fatalf("stream=%v not served uncached: %d %s", stream, status, body)
		}
		id := "resp_interaction_1"
		if stream {
			id = "resp_interaction_2"
		}
		status, body, _ = post(t, srv, "Bearer visible-refusal", fmt.Sprintf(`{"input":"B","previous_response_id":%q}`, id))
		if status != 400 || !strings.Contains(body, "previous_response_not_found") {
			t.Fatalf("uncached id must not be continuable: %d %s", status, body)
		}
	}
}
func TestReviewStoreRefusalServesUncachedWS(t *testing.T) {
	_, srv, _ := newLocalTestKeeper(t, func(c *config) { c.storePinBytes = 1 })
	c := dialWS(t, srv, "Bearer pin-refusal-owner")
	assertServedUncached(t, wsCreate(t, c, `{"type":"response.create","input":"A","store":false}`))
}
func reopenReadOnly(t *testing.T, s *store) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.db.Close(); err != nil {
		t.Fatal(err)
	}
	opts := boltOptions()
	opts.ReadOnly = true
	ro, err := bolt.Open(s.path, 0600, opts)
	if err != nil {
		t.Fatal(err)
	}
	s.db = ro
}
func TestReviewFailedReplacementPreservesOldEntry(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "same-id", o, "", items(`"A"`))
	reopenReadOnly(t, s)
	if _, err := s.put("same-id", o, "", items(`"B"`)); err != bolt.ErrDatabaseReadOnly {
		t.Fatalf("expected real transaction rejection: %v", err)
	}
	roundtrip(t, s, "same-id", o, []string{`"A"`})
	inspectStore(t, s)
}
func TestReviewDeleteFailureKeepsDiskIndexAligned(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	mustReviewPut(t, s, "old", o, "", items(`"A"`))
	reopenReadOnly(t, s)
	if err := dropTestResponse(s, "old"); err != bolt.ErrDatabaseReadOnly {
		t.Fatalf("expected rejected delete: %v", err)
	}
	roundtrip(t, s, "old", o, []string{`"A"`})
	inspectStore(t, s)
}
func TestReviewConcurrentCompactionAndLookup(t *testing.T) {
	s := newTestStore(t, time.Hour, 1<<20, 1<<20, 1<<19, 32<<10)
	o := [32]byte{1}
	mustReviewPut(t, s, "stable", o, "", items(`"A"`))
	pin, release, err := s.putPinned(o, "", "", items(`"A"`))
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := 0; i < 300; i++ {
			roundtrip(t, s, "stable", o, []string{`"A"`})
			got, ok := s.getPinned(pin, o)
			if !ok {
				t.Error("pin lost")
				return
			}
			checkItems(t, got, []string{`"A"`})
		}
	})
	for i := 0; i < 30; i++ {
		raw, _ := json.Marshal(strings.Repeat(fmt.Sprint(i), 8<<10))
		mustReviewPut(t, s, "churn", o, "", []json.RawMessage{raw})
		if err := dropTestResponse(s, "churn"); err != nil {
			t.Fatal(err)
		}
		n, rel, err := s.putPinned(o, "", "", items(`"temporary"`))
		if err != nil || n == "" {
			t.Fatal(err)
		}
		rel()
		s.mu.Lock()
		err = s.compactLocked()
		s.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
	roundtrip(t, s, "stable", o, []string{`"A"`})
	inspectStore(t, s)
}

// A keeper-side failure after the upstream started a response must end that
// response in protocol terms (response.failed with the same public id), so any
// client or gateway in front sees the turn terminate; the socket stays usable.
func TestStartedTurnFailureIsTerminalResponseFailed(t *testing.T) {
	fake := &fakeUpstream{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte("CUT")) {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, `data: {"type":"response.created","response":{"id":"interaction_cut","status":"in_progress","output":[]}}`+"\n\n")
			return // stream ends before any terminal event
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fake.ServeHTTP(w, r)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newLocalTestKeeper(t, func(c *config) { c.upstream = u })
	c := dialWS(t, srv, "Bearer terminal-failure")
	ev := wsCreate(t, c, `{"type":"response.create","input":"CUT"}`)
	resp, _ := ev["response"].(map[string]any)
	if ev["type"] != "response.failed" || resp["status"] != "failed" || resp["id"] != "resp_interaction_cut" {
		t.Fatalf("started turn not terminated with response.failed: %v", ev)
	}
	if ev := wsCreate(t, c, `{"type":"response.create","input":"B"}`); ev["type"] != "response.completed" {
		t.Fatalf("socket unusable after terminal failure: %v", ev)
	}
	_, body, _ := post(t, srv, "Bearer terminal-failure", `{"input":"CUT","stream":true}`)
	if !strings.Contains(body, "event: response.failed\ndata: ") || !strings.Contains(body, `"resp_interaction_cut"`) || strings.Contains(body, "event: error") {
		t.Fatalf("SSE started turn not terminated with response.failed: %s", body)
	}
}

// A failure before the upstream started any response stays a request error.
func TestUnstartedTurnFailureIsRequestError(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, `data: {"type":"response.output_text.delta","delta":"x"}`+"\n\n")
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, _ := newLocalTestKeeper(t, func(c *config) { c.upstream = u })
	if ev := wsCreate(t, dialWS(t, srv, "Bearer unstarted"), `{"type":"response.create","input":"A"}`); ev["type"] != "error" {
		t.Fatalf("unstarted failure must be a request error: %v", ev)
	}
}

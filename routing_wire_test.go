package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type routedRequest struct{ backend, authorization, body string }

type routingFixture struct {
	mu       sync.Mutex
	requests []routedRequest
	lookups  atomic.Int32
	failed   atomic.Bool
	cpa      *fakeUpstream
	aisix    *fakeUpstream
}

func newRoutingFixture(t *testing.T) (*routingFixture, *httptest.Server, *keeper) {
	t.Helper()
	f := &routingFixture{cpa: &fakeUpstream{}, aisix: &fakeUpstream{n: 100}}
	backend := func(name string, fake *fakeUpstream) *httptest.Server {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Error(err)
			}
			f.mu.Lock()
			f.requests = append(f.requests, routedRequest{name, r.Header.Get("Authorization"), string(body)})
			f.mu.Unlock()
			if strings.Contains(string(body), `"backend-error"`) {
				w.WriteHeader(502)
				io.WriteString(w, `{"error":{"code":"backend_error","message":"upstream detail"}}`)
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))
			fake.ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)
		return srv
	}
	cpa := backend("cpa", f.cpa)
	aisix := backend("aisix", f.aisix)
	index := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.lookups.Add(1)
		if r.Header.Get("Authorization") != "" {
			t.Error("routing index received caller credentials")
		}
		if f.failed.Load() {
			w.WriteHeader(500)
			return
		}
		decision := r.URL.Query().Get("model")
		switch decision {
		case "missing":
			decision = "not_found"
		case "backend-error":
			decision = "cpa"
		case "cpa", "aisix", "unavailable":
		default:
			decision = "not_found"
		}
		fmt.Fprintf(w, `{"decision":%q,"generation":7}`, decision)
	}))
	t.Cleanup(index.Close)
	_, srv, k := newTestKeeper(t, func(c *config) {
		c.upstream = nil
		c.routingIndex = routingURL(t, index.URL+"/routing-index")
		c.upstreamCPA = routingURL(t, cpa.URL)
		c.upstreamAISIX = routingURL(t, aisix.URL)
		c.cpaAPIKey = "sentinel-cpa-secret"
	})
	return f, srv, k
}

func (f *routingFixture) snapshot() []routedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]routedRequest(nil), f.requests...)
}

func TestRoutingHTTPAndSSEErrorsBeforeHistory(t *testing.T) {
	f, srv, k := newRoutingFixture(t)
	for _, stream := range []bool{false, true} {
		for _, tc := range []struct {
			model, code string
			status      int
		}{
			{"missing", "model_not_found", 404},
			{"unavailable", "service_unavailable", 503},
		} {
			t.Run(fmt.Sprintf("%s/stream=%t", tc.model, stream), func(t *testing.T) {
				before := f.lookups.Load()
				status, body, header := post(t, srv, "Bearer sentinel-caller", fmt.Sprintf(`{"model":%q,"stream":%t,"previous_response_id":"never-existed","input":[]}`, tc.model, stream))
				if status != tc.status || !strings.Contains(body, tc.code) || strings.Contains(body, "previous_response_not_found") || f.lookups.Load() != before+1 {
					t.Fatalf("status=%d body=%s lookups=%d", status, body, f.lookups.Load()-before)
				}
				if stream && (!strings.Contains(header.Get("Content-Type"), "text/event-stream") || !strings.HasPrefix(body, "event: error\ndata: ") || strings.Contains(body, "response.completed") || strings.Contains(body, "[DONE]")) {
					t.Fatalf("invalid SSE route failure: %s", body)
				}
				if !stream && !strings.Contains(header.Get("Content-Type"), "application/json") {
					t.Fatalf("invalid JSON error: %s", header)
				}
			})
		}
	}
	if len(f.snapshot()) != 0 {
		t.Fatal("route rejection reached upstream")
	}
	if n, size := k.store.stats(); n != 0 || size != 0 {
		t.Fatal("route rejection retained history")
	}
}

func TestRoutingMalformedRequestNeverLooksUp(t *testing.T) {
	f, srv, _ := newRoutingFixture(t)
	for _, body := range []string{
		"not-json", "[]", "null", "{}", `{"model":""}`, `{"model":42}`, `{"model":null}`, `{"model":[]}`,
		`{"model":"cpa"} {}`, `{"model":"cpa","nested":[}`, `{"model":"` + strings.Repeat("m", maxModelBytes+1) + `"}`,
		`{"model":"cpa","nested":` + strings.Repeat("[", 10001) + `0` + strings.Repeat("]", 10001) + `}`,
	} {
		status, _, _ := post(t, srv, "Bearer caller", body)
		if status != 400 {
			t.Fatalf("malformed request status=%d", status)
		}
	}
	if f.lookups.Load() != 0 || len(f.snapshot()) != 0 {
		t.Fatal("malformed input reached routing/backend")
	}
	status, _, _ := post(t, srv, "Bearer caller", `{"model":"missing","input":[{"model":"missing"}],"model":"cpa"}`)
	if status != 200 || f.lookups.Load() != 1 {
		t.Fatal("last top-level model did not match existing JSON semantics")
	}
}

func TestRoutingCredentialsOwnerAndNoBackendRetry(t *testing.T) {
	f, srv, _ := newRoutingFixture(t)
	var logs bytes.Buffer
	previousLog := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previousLog)
	status, body, _ := post(t, srv, "Bearer sentinel-owner-a", `{"model":"cpa","input":"first"}`)
	if status != 200 {
		t.Fatalf("first status=%d body=%s", status, body)
	}
	var response struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &response); err != nil || response.ID == "" {
		t.Fatalf("id missing: %s", body)
	}
	idA := response.ID
	status, body, _ = post(t, srv, "Bearer sentinel-owner-b", `{"model":"cpa","input":"separate"}`)
	if status != 200 || json.Unmarshal([]byte(body), &response) != nil || response.ID == idA {
		t.Fatalf("second owner response=%d %s", status, body)
	}
	for _, foreign := range []struct{ auth, id string }{{"Bearer sentinel-owner-b", idA}, {"Bearer sentinel-owner-a", response.ID}} {
		status, body, _ = post(t, srv, foreign.auth, fmt.Sprintf(`{"model":"cpa","input":"foreign","previous_response_id":%q}`, foreign.id))
		if status != 400 || !strings.Contains(body, "previous_response_not_found") || len(f.snapshot()) != 2 {
			t.Fatalf("CPA credential merged owners: %d %s", status, body)
		}
	}
	status, body, _ = post(t, srv, "Bearer sentinel-owner-a", fmt.Sprintf(`{"model":"aisix","input":"next","previous_response_id":%q,"stream":true}`, idA))
	if status != 200 || completedID(t, body) == "" || len(f.aisix.last()) != 4 {
		t.Fatalf("cross-backend continuation failed: %d %s", status, body)
	}
	requests := f.snapshot()
	if requests[0].authorization != "Bearer sentinel-cpa-secret" || requests[1].authorization != "Bearer sentinel-cpa-secret" || requests[2].authorization != "Bearer sentinel-owner-a" {
		t.Fatal("backend credentials incorrect")
	}
	status, body, _ = post(t, srv, "Bearer sentinel-owner-a", `{"model":"backend-error","input":[]}`)
	if status != 502 || !strings.Contains(body, "backend_error") || len(f.snapshot()) != 4 || f.snapshot()[3].backend != "cpa" {
		t.Fatal("backend error replayed or rewritten")
	}
	f.failed.Store(true)
	if status, _, _ := post(t, srv, "Bearer sentinel-owner-a", `{"model":"cpa","input":[]}`); status != 200 {
		t.Fatal("fresh LKG did not serve outage")
	}
	for _, secret := range []string{"sentinel-cpa-secret", "sentinel-owner-a", "sentinel-owner-b"} {
		if strings.Contains(logs.String(), secret) {
			t.Fatal("credential leaked into log")
		}
	}
}

func TestRoutingWSPerTurnKeepsPrivatePinOnError(t *testing.T) {
	f, srv, k := newRoutingFixture(t)
	conn := dialWS(t, srv, "Bearer sentinel-ws-caller")
	for _, body := range []string{"not-json", `{"type":"response.create","model":42}`} {
		event := wsCreate(t, conn, body)
		if event["type"] != "error" || event["status"] != float64(400) || f.lookups.Load() != 0 {
			t.Fatalf("malformed WS turn=%v", event)
		}
	}
	first := wsCreate(t, conn, `{"type":"response.create","model":"cpa","input":"first","store":false}`)
	if first["type"] != "response.completed" {
		t.Fatalf("first turn=%v", first)
	}
	id := responseID(first)
	for _, model := range []string{"missing", "unavailable"} {
		event := wsCreate(t, conn, fmt.Sprintf(`{"type":"response.create","model":%q,"previous_response_id":%q,"input":"rejected"}`, model, id))
		want := float64(404)
		if model == "unavailable" {
			want = 503
		}
		if event["type"] != "error" || event["status"] != want {
			t.Fatalf("route error=%v", event)
		}
	}
	last := wsCreate(t, conn, fmt.Sprintf(`{"type":"response.create","model":"aisix","previous_response_id":%q,"input":"next","store":false}`, id))
	if last["type"] != "response.completed" || len(f.aisix.last()) != 4 || f.lookups.Load() != 4 {
		t.Fatalf("socket/pin not preserved: %v lookups=%d", last, f.lookups.Load())
	}
	requests := f.snapshot()
	if len(requests) != 2 || requests[0].backend != "cpa" || requests[1].backend != "aisix" || requests[0].authorization != "Bearer sentinel-cpa-secret" || requests[1].authorization != "Bearer sentinel-ws-caller" {
		t.Fatalf("wrong WS routing or credentials: count=%d", len(requests))
	}
	if n, _ := k.store.stats(); n != 0 {
		t.Fatal("store:false WS turns registered aliases")
	}
}

func TestRoutingStripsOnlyTopLevelRetentionGolden(t *testing.T) {
	f, srv, _ := newRoutingFixture(t)
	const item = `{"type" : "message", "text":"<&>\u00e9", "n":1e+03}`
	const extra = `{"prompt_cache_retention" : "nested-kept", "escaped":"\u0061"}`
	for _, ws := range []bool{false, true} {
		body := `{"model":"\u0063pa","prompt_cache_retention":{"nested":[1,true,null]},"input":[` + item + `],"extra":` + extra + `,"stream":false}`
		stream := "false"
		if ws {
			body = `{"type":"response.create",` + body[1:]
			conn := dialWS(t, srv, "Bearer caller")
			if event := wsCreate(t, conn, body); event["type"] != "response.completed" {
				t.Fatalf("WS turn=%v", event)
			}
			stream = "true"
		} else if status, result, _ := post(t, srv, "Bearer caller", body); status != 200 {
			t.Fatalf("HTTP status=%d body=%s", status, result)
		}
		requests := f.snapshot()
		want := `{"input":[` + item + `],"extra":` + extra + `,"model":"\u0063pa","stream":` + stream + `}`
		if got := requests[len(requests)-1].body; got != want {
			t.Fatalf("ws=%t\n got: %s\nwant: %s", ws, got, want)
		}
	}
}

func TestRoutingLookupDoesNotHoldMaterializationPhase(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	index := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		io.WriteString(w, `{"decision":"cpa","generation":1}`)
	}))
	defer index.Close()
	_, _, k := newTestKeeper(t, func(c *config) {
		c.routingIndex = routingURL(t, index.URL)
		c.upstreamCPA = c.upstream
	})
	raw, err := k.spoolRequest(context.Background(), strings.NewReader(`{"model":"cpa","input":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	done := make(chan *apiError, 1)
	go func() { _, _, err := k.routeRequest(context.Background(), raw, false); done <- err }()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("routing request did not start")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	leave, err := k.work.enter(ctx)
	cancel()
	close(release)
	if err != nil {
		t.Fatal("network lookup held materialization phase")
	}
	leave()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

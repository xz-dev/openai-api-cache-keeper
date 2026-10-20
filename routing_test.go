package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func routingURL(t *testing.T, address string) *url.URL {
	t.Helper()
	u, err := url.Parse(address)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func TestRoutingIndexContract(t *testing.T) {
	data, err := os.ReadFile("testdata/routing-index.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Name, Body, Decision string }
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			if got := parseRoutingDecision([]byte(tc.Body)); got != tc.Decision {
				t.Fatalf("decision=%q want=%q", got, tc.Decision)
			}
		})
	}
	if got := parseRoutingDecision([]byte(`{"decision":"cpa","generation":7}` + strings.Repeat(" ", maxRoutingBody))); got != "" {
		t.Fatal("oversized index accepted")
	}
}

type routingRoundTrip func(*http.Request) (*http.Response, error)

func (f routingRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestRoutingFallbackLifecycleAndBound(t *testing.T) {
	r := newRoutingClient(routingURL(t, "http://index/routing-index"))
	now := time.Unix(100, 0)
	r.now = func() time.Time { return now }
	body := `{"decision":"cpa","generation":7}`
	calls := 0
	r.client.Transport = routingRoundTrip(func(req *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}, nil
	})
	decide := func(model, want string, status int) {
		t.Helper()
		before := calls
		got, err := r.decide(context.Background(), model)
		if calls != before+1 || got != want || (err == nil) != (status == 0) || err != nil && err.status != status {
			t.Fatalf("model=%q got=%q err=%v calls=%d", model, got, err, calls-before)
		}
	}
	decide("exact", "cpa", 0)
	body = "bad"
	decide("different", "", 503)
	now = now.Add(14 * time.Minute)
	var logs bytes.Buffer
	previousLog := log.Writer()
	log.SetOutput(&logs)
	decide("exact", "cpa", 0)
	log.SetOutput(previousLog)
	if strings.Count(logs.String(), "routing-index fallback") != 1 || !strings.Contains(logs.String(), `model="exact" age=14m0s`) {
		t.Fatalf("fallback warning=%q", logs.String())
	}
	// Fallback must not refresh the stored decision's age.
	now = now.Add(time.Minute)
	decide("exact", "", 503)
	for _, outcome := range []string{"not_found", "unavailable"} {
		body = `{"decision":"aisix","generation":7}`
		decide("exact", "aisix", 0)
		body = fmt.Sprintf(`{"decision":%q,"generation":7}`, outcome)
		status := 503
		if outcome == "not_found" {
			status = 404
		}
		decide("exact", "", status)
		body = "bad"
		decide("exact", "", 503)
	}
	body = `{"decision":"cpa","generation":7}`
	for i := 0; i <= maxRoutingEntries; i++ {
		now = now.Add(time.Second)
		decide(fmt.Sprint(i), "cpa", 0)
	}
	if len(r.lastGood) != maxRoutingEntries {
		t.Fatalf("entries=%d", len(r.lastGood))
	}
	if _, present := r.lastGood["0"]; present {
		t.Fatal("oldest decision was not evicted")
	}
}

func TestRoutingLookupFailureAndExactModel(t *testing.T) {
	for _, mode := range []string{"ok", "status", "malformed", "oversized", "truncated", "redirect", "headers-timeout", "body-timeout"} {
		t.Run(mode, func(t *testing.T) {
			var calls, redirected atomic.Int32
			redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { redirected.Add(1) }))
			defer redirect.Close()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if r.Method != "GET" || r.URL.Path != "/routing-index" || r.URL.Query().Get("model") != "axis/model + exact" || r.Header.Get("Authorization") != "" {
					t.Error("incorrect routing request or leaked credential")
				}
				switch mode {
				case "status":
					w.WriteHeader(500)
				case "malformed":
					io.WriteString(w, "invalid")
				case "oversized":
					io.WriteString(w, strings.Repeat("x", maxRoutingBody+1))
				case "truncated":
					w.Header().Set("Content-Length", "100")
					io.WriteString(w, `{"decision":"cpa","generation":7}`)
				case "redirect":
					http.Redirect(w, r, redirect.URL, http.StatusFound)
				case "headers-timeout":
					<-r.Context().Done()
				case "body-timeout":
					io.WriteString(w, `{`)
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				default:
					io.WriteString(w, `{"decision":"cpa","generation":7}`)
				}
			}))
			defer srv.Close()
			r := newRoutingClient(routingURL(t, srv.URL+"/routing-index"))
			start := time.Now()
			decision, err := r.decide(context.Background(), "axis/model + exact")
			if mode == "ok" {
				if err != nil || decision != "cpa" {
					t.Fatalf("%q %v", decision, err)
				}
			} else if err == nil || err.status != 503 {
				t.Fatalf("expected 503, got %q %v", decision, err)
			}
			if calls.Load() != 1 || redirected.Load() != 0 {
				t.Fatalf("requests=%d redirects=%d", calls.Load(), redirected.Load())
			}
			if elapsed := time.Since(start); elapsed > 2500*time.Millisecond {
				t.Fatalf("lookup exceeded timeout budget: %s", elapsed)
			}
		})
	}
}

func TestRoutingLookupUsesHTTP1WithoutReplay(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		calls.Add(1)
		if req.ProtoMajor != 1 {
			t.Error("routing lookup negotiated replay-capable HTTP/2")
		}
		io.WriteString(w, `{"decision":"cpa","generation":7}`)
	}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()
	r := newRoutingClient(routingURL(t, srv.URL))
	r.client.Transport.(*http.Transport).TLSClientConfig = srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	if decision, err := r.decide(context.Background(), "model"); err != nil || decision != "cpa" || calls.Load() != 1 {
		t.Fatalf("decision=%q err=%v calls=%d", decision, err, calls.Load())
	}
}

func TestRoutingConnectionBudgetsAndCancellation(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	conn := routingConn{a}
	start := time.Now()
	if _, err := conn.Write([]byte("blocked")); err == nil || time.Since(start) > time.Second {
		t.Fatalf("write deadline missing: %v", err)
	}
	start = time.Now()
	if _, err := conn.Read(make([]byte, 1)); err == nil || time.Since(start) > 2*time.Second {
		t.Fatalf("read deadline missing: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := ln.Addr().String()
	ln.Close()
	r := newRoutingClient(routingURL(t, "http://"+address+"/routing-index"))
	if _, err := r.decide(context.Background(), "missing"); err == nil || err.status != 503 {
		t.Fatalf("connect failure=%v", err)
	}
	r.lastGood["known"] = routingDecision{"cpa", time.Now()}
	if decision, err := r.decide(context.Background(), "known"); err != nil || decision != "cpa" {
		t.Fatalf("connection failure with LKG=%q %v", decision, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := r.decide(ctx, "known"); err == nil {
		t.Fatal("cancellation fell back and forwarded")
	}
}

func TestRoutingAuthoritativeNegativeSurvivesCallerCancellation(t *testing.T) {
	r := newRoutingClient(routingURL(t, "http://index/routing-index"))
	r.lastGood["model"] = routingDecision{"cpa", time.Now()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r.client.Transport = routingRoundTrip(func(*http.Request) (*http.Response, error) {
		cancel()
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"decision":"not_found","generation":7}`))}, nil
	})
	if _, err := r.decide(ctx, "model"); err == nil {
		t.Fatal("canceled caller forwarded")
	}
	if _, exists := r.lastGood["model"]; exists {
		t.Fatal("caller cancellation retained an authoritatively deleted route")
	}
}

func TestRoutingConcurrentDecisions(t *testing.T) {
	r := newRoutingClient(routingURL(t, "http://index/routing-index"))
	r.client.Transport = routingRoundTrip(func(req *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"decision":"cpa","generation":7}`))}, nil
	})
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			for j := 0; j < 32; j++ {
				if _, err := r.decide(context.Background(), fmt.Sprint((i+j)%8)); err != nil {
					t.Error(err)
				}
			}
		}(i)
	}
	group.Wait()
	if len(r.lastGood) != 8 {
		t.Fatalf("entries=%d", len(r.lastGood))
	}
}

func TestRoutingConfiguration(t *testing.T) {
	vars := []string{"UPSTREAM_URL", "ROUTING_INDEX_URL", "UPSTREAM_CPA_URL", "UPSTREAM_AISIX_URL", "CPA_API_KEY"}
	for _, name := range vars {
		t.Setenv(name, "")
	}
	valid := map[string]string{"ROUTING_INDEX_URL": "http://index/routing-index", "UPSTREAM_CPA_URL": "http://cpa:8317", "UPSTREAM_AISIX_URL": "http://aisix:3000", "CPA_API_KEY": "sentinel-cpa-key"}
	for name, value := range valid {
		t.Setenv(name, value)
	}
	cfg, err := loadConfig()
	if err != nil || cfg.upstream != nil || cfg.routingIndex == nil || cfg.upstreamCPA == nil || cfg.upstreamAISIX == nil || cfg.cpaAPIKey != valid["CPA_API_KEY"] {
		t.Fatalf("routed config rejected: %v", err)
	}
	for name, value := range valid {
		t.Setenv(name, "")
		if _, err := loadConfig(); err == nil {
			t.Fatalf("partial config accepted without %s", name)
		}
		t.Setenv(name, value)
	}
	t.Setenv("UPSTREAM_URL", "http://legacy")
	if _, err := loadConfig(); err == nil {
		t.Fatal("mixed modes accepted")
	}
	t.Setenv("UPSTREAM_URL", "")
	for _, bad := range []string{"ftp://index", "http://key:secret@index", "http://index?q=secret", "http://index#secret"} {
		t.Setenv("ROUTING_INDEX_URL", bad)
		if _, err := loadConfig(); err == nil || strings.Contains(err.Error(), "secret") {
			t.Fatalf("URL validation did not reject/redact invalid configuration: %v", err)
		}
	}
	t.Setenv("ROUTING_INDEX_URL", valid["ROUTING_INDEX_URL"])
	t.Setenv("CPA_API_KEY", "sentinel\ninvalid")
	if _, err := loadConfig(); err == nil || strings.Contains(err.Error(), "sentinel") {
		t.Fatal("key validation missing or leaked key")
	}
	for name := range valid {
		t.Setenv(name, "")
	}
	t.Setenv("UPSTREAM_URL", "http://legacy/base")
	cfg, err = loadConfig()
	if err != nil || cfg.upstream == nil || cfg.routingIndex != nil {
		t.Fatalf("single upstream compatibility broken: %v", err)
	}
}

package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOwnerMatchesSub2APICredentialPrecedence(t *testing.T) {
	// Routed mode keeps per-credential ownership: Bearer, then x-api-key,
	// then x-goog-api-key, matching Sub2API 2ddb824 api_key_auth.go.
	_, _, k := newKeeperFixture(t, false, func(c *config) {
		c.upstream = nil
		c.upstreamCPA = &url.URL{Scheme: "http", Host: "cpa:1"}
		c.upstreamAISIX = &url.URL{Scheme: "http", Host: "aisix:1"}
	})
	for name, headers := range map[string]map[string]string{
		"bearer":              {"Authorization": "Bearer owner"},
		"bearer-normalized":   {"Authorization": "bEaReR   owner\t"},
		"api-key":             {"X-API-Key": "owner"},
		"goog-key":            {"X-Goog-API-Key": "owner"},
		"bearer-precedence":   {"Authorization": "Bearer owner", "X-API-Key": "foreign", "X-Goog-API-Key": "foreign"},
		"api-precedence":      {"Authorization": "Basic ignored", "X-API-Key": "owner", "X-Goog-API-Key": "foreign"},
		"empty-bearer":        {"Authorization": "Bearer   ", "X-API-Key": "owner"},
		"goog-fallback":       {"Authorization": "Basic ignored", "X-Goog-API-Key": "owner"},
		"connection-stripped": {"Connection": "X-API-Key", "X-API-Key": "shared-untrusted-marker", "X-Goog-API-Key": "owner"},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/v1/responses/resp_test", nil)
			for name, value := range headers {
				req.Header.Set(name, value)
			}
			if k.ownerOf(req) != sha256.Sum256([]byte("owner")) {
				t.Fatal("owner does not identify the credential selected by Sub2API")
			}
		})
	}
}

// TestSingleUpstreamNoCredentialIsolation asserts that in single-upstream mode
// every stored id is continuable regardless of the presenting credential: the
// architecture provides isolation, and response ids are unguessable.
func TestSingleUpstreamNoCredentialIsolation(t *testing.T) {
	_, srv, _ := newTestKeeper(t)
	status, body, _ := post(t, srv, "Bearer a", `{"model":"m","input":"hi","stream":true}`)
	if status != http.StatusOK {
		t.Fatalf("create status=%d body=%s", status, body)
	}
	var id string
	_ = readSSE(strings.NewReader(body), nil, func(data []byte) error {
		var ev struct {
			Response struct{ ID string } `json:"response"`
		}
		_ = json.Unmarshal(data, &ev)
		if ev.Response.ID != "" {
			id = ev.Response.ID
		}
		return nil
	})
	if id == "" {
		t.Fatalf("no response id in %q", body)
	}
	status, body, _ = post(t, srv, "Bearer b", fmt.Sprintf(`{"model":"m","input":"next","stream":true,"previous_response_id":%q}`, id))
	if status != http.StatusOK {
		t.Fatalf("different credential could not continue stored id: status=%d body=%s", status, body)
	}
}

// TestBackgroundAlternateCredentials exercises the alternate-credential
// header paths in single-upstream mode, where there is no per-credential
// isolation: every stored id is continuable by any caller.
func TestBackgroundAlternateCredentials(t *testing.T) {
	for _, carrier := range []string{"X-API-Key", "X-Goog-API-Key", "Connection-stripped"} {
		t.Run(carrier, func(t *testing.T) {
			var creates atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Connection") != "" {
					t.Errorf("hop-by-hop headers reached upstream: %v", r.Header)
				}
				var key string
				if scheme, token, ok := strings.Cut(r.Header.Get("Authorization"), " "); ok && strings.EqualFold(scheme, "Bearer") {
					key = strings.TrimSpace(token)
				}
				if key == "" {
					key = r.Header.Get("X-API-Key")
				}
				if key == "" {
					key = r.Header.Get("X-Goog-API-Key")
				}
				if key != "owner" && key != "foreign" {
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
				if r.URL.Path == "/v1/sub2api/billing" {
					_, _ = io.WriteString(w, `{}`)
					return
				}
				if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
					http.NotFound(w, r)
					return
				}
				creates.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_auth\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_auth\",\"status\":\"completed\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"private\"}]}]}}\n\n")
			}))
			t.Cleanup(up.Close)
			u, _ := url.Parse(up.URL)
			_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
			client := &http.Client{Timeout: 2 * time.Second}
			request := func(method, path, header, key, body string) (int, string) {
				t.Helper()
				req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				req.Header.Set("Content-Type", "application/json")
				if header == "Connection-stripped" {
					req.Header.Set("Connection", "X-API-Key")
					req.Header.Set("X-API-Key", "shared-untrusted-marker")
					req.Header.Set("X-Goog-API-Key", key)
				} else {
					req.Header.Set(header, key)
				}
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer resp.Body.Close()
				data, err := io.ReadAll(resp.Body)
				if err != nil {
					t.Fatal(err)
				}
				return resp.StatusCode, string(data)
			}
			_ = carrier // credential precedence test below uses routed mode
			status, body := request(http.MethodPost, "/v1/responses", carrier, "owner", `{"model":"m","input":"A","background":true,"stream":true}`)
			if status != http.StatusOK {
				t.Fatalf("create status=%d body=%s", status, body)
			}
			var id string
			if err := readSSE(strings.NewReader(body), nil, func(data []byte) error {
				var event struct {
					Response struct {
						ID string `json:"id"`
					} `json:"response"`
				}
				if err := json.Unmarshal(data, &event); err != nil {
					return err
				}
				if event.Response.ID != "" {
					id = event.Response.ID
				}
				return nil
			}); err != nil || id == "" {
				t.Fatalf("response identity missing: %s err=%v", body, err)
			}
			// Single-upstream mode has no per-credential isolation: any id is
			// continuable. A "foreign" credential can read the background task.
			status, body = request(http.MethodGet, "/v1/responses/"+id, carrier, "foreign", "")
			if status != http.StatusOK {
				t.Errorf("shared-owner GET status=%d body=%s", status, body)
			}
			status, body = request(http.MethodGet, "/v1/responses/"+id, "Authorization", "Bearer owner", "")
			if status != http.StatusOK || !strings.Contains(body, "private") {
				t.Errorf("same key via Bearer lost ownership: status=%d body=%s", status, body)
			}
			// And a different credential can continue the stored id.
			status, body = request(http.MethodPost, "/v1/responses", carrier, "foreign", fmt.Sprintf(`{"model":"m","input":"B","previous_response_id":%q}`, id))
			if status != http.StatusOK {
				t.Errorf("foreign credential could not continue: status=%d body=%s", status, body)
			}
			if creates.Load() != 2 {
				t.Errorf("creates=%d", creates.Load())
			}
		})
	}
}

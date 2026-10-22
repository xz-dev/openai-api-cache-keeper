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
	// Sub2API 2ddb824 api_key_auth.go: Bearer, then x-api-key, then x-goog-api-key.
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
			if ownerOf(req) != sha256.Sum256([]byte("owner")) {
				t.Fatal("owner does not identify the credential selected by Sub2API")
			}
		})
	}
}

func TestBackgroundAlternateCredentialsIsolateOwners(t *testing.T) {
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
			for _, op := range []struct{ method, suffix string }{
				{http.MethodGet, ""}, {http.MethodGet, "?stream=true&starting_after=0"}, {http.MethodPost, "/cancel"},
			} {
				status, body := request(op.method, "/v1/responses/"+id+op.suffix, carrier, "foreign", "")
				if status != http.StatusNotFound || strings.Contains(body, "private") {
					t.Errorf("foreign %s %s exposed task: status=%d body=%s", op.method, op.suffix, status, body)
				}
			}
			status, body = request(http.MethodGet, "/v1/responses/"+id, "Authorization", "Bearer owner", "")
			if status != http.StatusOK || !strings.Contains(body, "private") {
				t.Errorf("same key via Bearer lost ownership: status=%d body=%s", status, body)
			}
			status, body = request(http.MethodPost, "/v1/responses", carrier, "foreign", fmt.Sprintf(`{"model":"m","input":"B","previous_response_id":%q}`, id))
			if status != http.StatusBadRequest || creates.Load() != 1 {
				t.Errorf("foreign checkpoint reached generation: status=%d creates=%d body=%s", status, creates.Load(), body)
			}
		})
	}
}

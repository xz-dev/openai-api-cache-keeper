package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestBackgroundPreservesQueryAuthenticationInputs(t *testing.T) {
	var creates atomic.Int32
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Upstream-Query", r.URL.RawQuery)
		// Sub2API 2ddb824 rejects query credentials before normal key validation.
		if strings.TrimSpace(r.URL.Query().Get("key")) != "" || strings.TrimSpace(r.URL.Query().Get("api_key")) != "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, `{"error":"api_key_in_query_deprecated"}`)
			return
		}
		if r.Header.Get("Authorization") != "Bearer owner" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/v1/sub2api/billing" {
			_, _ = io.WriteString(w, `{}`)
			return
		}
		creates.Add(1)
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_query\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_query\",\"status\":\"completed\",\"output\":[]}}\n\n")
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	client := &http.Client{Timeout: 2 * time.Second}
	request := func(method, path, body string) (int, string, http.Header) {
		t.Helper()
		req, err := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Authorization", "Bearer owner")
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatal(err)
		}
		return resp.StatusCode, string(data), resp.Header
	}
	const createBody = `{"model":"m","input":"A","background":true,"stream":true}`
	status, body, header := request(http.MethodPost, "/v1/responses?opaque=%2F", createBody)
	if status != http.StatusOK || header.Get("X-Upstream-Query") != "opaque=%2F" {
		t.Errorf("opaque query changed: status=%d query=%q", status, header.Get("X-Upstream-Query"))
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
	for _, tc := range []struct{ method, path, body string }{
		{http.MethodPost, "/v1/responses?key=deprecated", createBody},
		{http.MethodGet, "/v1/responses/" + id + "?key=deprecated&stream=true", ""},
		{http.MethodPost, "/v1/responses/" + id + "/cancel?api_key=%20deprecated%20", ""},
	} {
		status, body, _ := request(tc.method, tc.path, tc.body)
		if status != http.StatusBadRequest || !strings.Contains(body, "api_key_in_query_deprecated") {
			t.Errorf("credential query was hidden: %s %s status=%d body=%s", tc.method, tc.path, status, body)
		}
	}
	if creates.Load() != 1 {
		t.Errorf("rejected query started generation: creates=%d", creates.Load())
	}
}

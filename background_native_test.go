package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestBackgroundNativeParentStaysIncremental(t *testing.T) {
	var creates atomic.Int32
	release := make(chan struct{})
	released := false
	defer func() {
		if !released {
			close(release)
		}
	}()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sub2api/billing" {
			_, _ = io.WriteString(w, `{}`)
			return
		}
		if r.Method != http.MethodPost {
			http.NotFound(w, r)
			return
		}
		n := creates.Add(1)
		body, _ := io.ReadAll(r.Body)
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(body, &doc); err != nil {
			t.Error(err)
		}
		if n == 2 {
			if string(doc["previous_response_id"]) != `"resp_native_1"` {
				t.Errorf("native parent changed: %s", body)
			}
			if strings.Contains(string(body), "FIRST_TURN") {
				t.Errorf("native history expanded: %s", body)
			}
			if string(doc["prompt_cache_retention"]) != `"24h"` || string(doc["unknown"]) != `{"raw":true}` {
				t.Errorf("input fields lost: %s", body)
			}
			if string(doc["background"]) == "true" {
				t.Error("lifecycle flag leaked upstream")
			}
		}
		if n == 3 && string(body) != `{"model":"m","input":"C","previous_response_id":"resp_native_2"}` {
			t.Errorf("native checkpoint changed: %s", body)
		}
		response := fmt.Sprintf(`{"id":"resp_native_%d","object":"response","status":"completed","store":true,"output":[]}`, n)
		if n != 2 {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, response)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_native_2\",\"object\":\"response\",\"status\":\"in_progress\",\"store\":true,\"output\":[]}}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = io.WriteString(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"second\"}\n\n")
		_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":%s}\n\n", response)
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	status, body, _ := post(t, srv, "Bearer owner", `{"model":"m","input":"FIRST_TURN"}`)
	if status != 200 || !strings.Contains(body, "resp_native_1") {
		t.Fatalf("native first turn: %d %s", status, body)
	}
	resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"B","previous_response_id":"resp_native_1","prompt_cache_retention":"24h","unknown":{"raw":true},"background":true,"stream":true}`)
	resp.Body.Close()
	close(release)
	released = true
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/responses/"+id+"?stream=true&starting_after=0", nil)
	req.Header.Set("Authorization", "Bearer owner")
	tail, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Body.Close()
	if tail.StatusCode != 200 {
		t.Fatalf("native-parent replay status=%d", tail.StatusCode)
	}
	count := 0
	if err := readSSE(tail.Body, nil, func(data []byte) error {
		count++
		var event struct {
			Type     string `json:"type"`
			Response struct {
				ID    string `json:"id"`
				Store bool   `json:"store"`
			} `json:"response"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			return err
		}
		if event.Type == "response.completed" && (event.Response.ID != id || !event.Response.Store) {
			t.Errorf("native checkpoint metadata changed: %s", data)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 2 || creates.Load() != 2 {
		t.Fatalf("replay events=%d total generations=%d", count, creates.Load())
	}
	status, body, _ = post(t, srv, "Bearer owner", `{"model":"m","input":"C","previous_response_id":"`+id+`"}`)
	if status != 200 || !strings.Contains(body, "resp_native_3") {
		t.Fatalf("native continuation: %d %s", status, body)
	}
	if entries, _ := k.store.stats(); entries != 0 {
		t.Fatalf("invented native parent history: %d entries", entries)
	}
}

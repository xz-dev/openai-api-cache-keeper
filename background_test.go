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
	"sync/atomic"
	"testing"
	"time"
)

func TestBackgroundDisconnectReplaysSameGeneration(t *testing.T) {
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
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{}`)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/v1/responses" {
			http.NotFound(w, r)
			return
		}
		generation := creates.Add(1)
		body, _ := io.ReadAll(r.Body)
		var request map[string]json.RawMessage
		if err := json.Unmarshal(body, &request); err != nil {
			t.Error(err)
		}
		if generation == 2 {
			var input []json.RawMessage
			if err := json.Unmarshal(request["input"], &input); err != nil || strings.Join(kinds(input), ",") != "user:A,assistant,user:B" {
				t.Errorf("real checkpoint history=%s err=%v", request["input"], err)
			}
		}
		if string(request["background"]) == "true" {
			t.Error("keeper lifecycle flag leaked upstream")
		}
		w.Header().Set("Content-Type", "text/event-stream")
		events := []string{
			`{"type":"response.created","response":{"id":"resp_upstream","object":"response","status":"in_progress","output":[]}}`,
			`{"type":"response.output_text.delta","item_id":"msg_1","output_index":0,"content_index":0,"delta":"hello","unknown_opaque":{"values":[1,null]}}`,
			`{"type":"response.reasoning_summary_text.delta","item_id":"rs_1","output_index":1,"summary_index":0,"delta":"reason"}`,
			`{"type":"response.function_call_arguments.delta","item_id":"fc_1","output_index":2,"delta":"{\"x\":1}"}`,
			`{"type":"response.completed","response":{"id":"resp_upstream","object":"response","status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"hello"}]}]}}`,
		}
		for i, event := range events {
			if i == 2 {
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			_, _ = fmt.Fprintf(w, "data: %s\n\n", event)
			w.(http.Flusher).Flush()
		}
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"model":"m","input":"A","background":true,"stream":true}`))
	req.Header.Set("Authorization", "Bearer owner")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	var accepted []json.RawMessage
	for len(accepted) < 2 {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.HasPrefix(line, "data: ") {
			accepted = append(accepted, json.RawMessage(strings.TrimSpace(strings.TrimPrefix(line, "data: "))))
		}
	}
	var created struct {
		Response struct {
			ID string `json:"id"`
		} `json:"response"`
	}
	if err := json.Unmarshal(accepted[0], &created); err != nil || created.Response.ID == "" {
		t.Fatalf("created=%s err=%v", accepted[0], err)
	}
	resp.Body.Close()
	close(release)
	released = true
	resume, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/responses/"+created.Response.ID+"?stream=true&starting_after=1", nil)
	resume.Header.Set("Authorization", "Bearer owner")
	tail, err := http.DefaultClient.Do(resume)
	if err != nil {
		t.Fatal(err)
	}
	defer tail.Body.Close()
	if tail.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(tail.Body)
		t.Fatalf("resume status=%d body=%s", tail.StatusCode, body)
	}
	if err := readSSE(tail.Body, nil, func(data []byte) error {
		accepted = append(accepted, append(json.RawMessage(nil), data...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"response.created", "response.output_text.delta", "response.reasoning_summary_text.delta", "response.function_call_arguments.delta", "response.completed"}
	if len(accepted) != len(want) {
		t.Fatalf("event count=%d: %s", len(accepted), accepted)
	}
	for i, raw := range accepted {
		var event struct {
			Type     string `json:"type"`
			Sequence int    `json:"sequence_number"`
		}
		if err := json.Unmarshal(raw, &event); err != nil || event.Type != want[i] || event.Sequence != i {
			t.Fatalf("event %d=%s err=%v", i, raw, err)
		}
	}
	if !strings.Contains(string(accepted[1]), `"unknown_opaque":{"values":[1,null]}`) {
		t.Fatalf("unknown field lost: %s", accepted[1])
	}
	if creates.Load() != 1 {
		t.Fatalf("upstream generations=%d", creates.Load())
	}
	status, body, _ := post(t, srv, "Bearer owner", `{"model":"m","input":"B","previous_response_id":"`+created.Response.ID+`"}`)
	if status != http.StatusOK {
		t.Fatalf("real checkpoint continuation status=%d body=%s", status, body)
	}
}

func openBackgroundTask(t *testing.T, srv *httptest.Server, body string) (*http.Response, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer owner")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	if resp.StatusCode != http.StatusOK {
		got, _ := io.ReadAll(resp.Body)
		t.Fatalf("background create status=%d body=%s", resp.StatusCode, got)
	}
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var event struct {
			Response struct {
				ID string `json:"id"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &event); err != nil || event.Response.ID == "" {
			t.Fatalf("created=%s err=%v", line, err)
		}
		return resp, event.Response.ID
	}
}

func TestBackgroundAuthCancelAndFailure(t *testing.T) {
	for _, mode := range []string{"cancel", "error", "truncated", "orphan", "budget", "identity"} {
		t.Run(mode, func(t *testing.T) {
			var authorized atomic.Bool
			authorized.Store(true)
			var creates atomic.Int32
			release := make(chan struct{}, 1)
			defer close(release)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/sub2api/billing" {
					if !authorized.Load() {
						w.WriteHeader(401)
						_, _ = io.WriteString(w, `{"error":"revoked"}`)
						return
					}
					_, _ = io.WriteString(w, `{}`)
					return
				}
				if creates.Add(1) != 1 {
					http.Error(w, "unexpected generation", 500)
					return
				}
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_upstream\",\"status\":\"in_progress\",\"output\":[]}}\n\n")
				w.(http.Flusher).Flush()
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				if mode == "error" {
					_, _ = io.WriteString(w, "data: {\"type\":\"error\",\"error\":{\"code\":\"upstream_failure\"}}\n\n")
				}
				if mode == "identity" {
					_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_other_generation\",\"status\":\"completed\",\"output\":[]}}\n\n")
				}
				if mode == "budget" {
					_, _ = fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":%q}\n\n", strings.Repeat("x", 1024))
				}
			}))
			t.Cleanup(up.Close)
			u, _ := url.Parse(up.URL)
			_, srv, k := newTestKeeper(t, func(c *config) {
				c.upstream = u
				if mode == "orphan" {
					c.ttl = 100 * time.Millisecond
				}
				if mode == "budget" {
					c.maxStoreBytes = 512
				}
			})
			resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
			k.cacheMu.Lock()
			task := k.tasks[id]
			k.cacheMu.Unlock()
			if task == nil {
				t.Fatal("missing real task")
			}
			resp.Body.Close()
			request := func(method, suffix, auth string) (int, string) {
				t.Helper()
				req, _ := http.NewRequest(method, srv.URL+"/v1/responses/"+id+suffix, nil)
				req.Header.Set("Authorization", auth)
				res, err := http.DefaultClient.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				defer res.Body.Close()
				body, err := io.ReadAll(res.Body)
				if err != nil {
					t.Fatal(err)
				}
				return res.StatusCode, string(body)
			}
			authorized.Store(false)
			if status, body := request("GET", "", "Bearer owner"); status != 401 || !strings.Contains(body, "revoked") {
				t.Fatalf("revocation: %d %s", status, body)
			}
			authorized.Store(true)
			if status, body := request("GET", "", "Bearer foreign"); status != 404 {
				t.Fatalf("foreign: %d %s", status, body)
			}
			if mode == "cancel" {
				for range 2 {
					if status, body := request("POST", "/cancel", "Bearer owner"); status != 200 || !strings.Contains(body, `"status":"cancelled"`) {
						t.Fatalf("cancel: %d %s", status, body)
					}
				}
			} else if mode != "orphan" {
				// Release without double-closing the channel in the deferred cleanup.
				release <- struct{}{}
			}
			select {
			case <-task.finished:
			case <-time.After(3 * time.Second):
				t.Fatal("upstream task did not stop")
			}
			status, body := request("GET", "?stream=true&starting_after=0", "Bearer owner")
			if (mode == "orphan" && status != 404) || (mode != "orphan" && status != 409) {
				t.Fatalf("failed replay: %d %s", status, body)
			}
			status, body, _ = post(t, srv, "Bearer owner", `{"model":"m","input":"B","previous_response_id":"`+id+`"}`)
			if status != 400 || !strings.Contains(body, "previous_response_not_found") {
				t.Fatalf("failed checkpoint reused: %d %s", status, body)
			}
			if creates.Load() != 1 {
				t.Fatalf("hidden generations=%d", creates.Load())
			}
			if entries, _ := k.store.stats(); entries != 0 {
				t.Fatalf("failed history retained: %d", entries)
			}
			k.cacheMu.Lock()
			bytes := k.eventBytes
			k.cacheMu.Unlock()
			if bytes != 0 {
				t.Fatalf("failed log retained: %d", bytes)
			}
		})
	}
}

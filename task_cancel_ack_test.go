package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestFinalJSONCancelAfterTerminalBeforeEOF(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sub2api/billing" {
			io.WriteString(w, `{}`)
			return
		}
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_up\",\"object\":\"response\",\"status\":\"completed\",\"output\":[{\"text\":%q}]}}\n\n", strings.Repeat("x", 128<<10))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	baseline := admissionSnapshot(k)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"input":"x","store":false,"background":true,"stream":true}`))
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var id string
	terminalSeen := errors.New("terminal seen")
	err = readSSE(resp.Body, nil, func(data []byte) error {
		var event struct {
			Response struct {
				ID string `json:"id"`
			} `json:"response"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			return err
		}
		id = event.Response.ID
		return terminalSeen
	})
	resp.Body.Close()
	if !errors.Is(err, terminalSeen) || id == "" {
		t.Fatalf("terminal fixture failed: %v", err)
	}
	k.cacheMu.Lock()
	task := k.tasks[id]
	k.cacheMu.Unlock()
	if task == nil {
		t.Fatal("real task missing")
	}
	defer task.expire()
	task.mu.Lock()
	complete, done, readStop := task.complete, task.done, task.readStop != nil
	task.mu.Unlock()
	if !complete || done || !readStop {
		t.Fatalf("wrong live phase: complete=%v done=%v readStop=%v", complete, done, readStop)
	}
	ack, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses/"+id+"/cancel", nil)
	res, ackErr := srv.Client().Do(ack)
	var body []byte
	status := 0
	if ackErr == nil {
		status = res.StatusCode
		body, ackErr = io.ReadAll(res.Body)
		res.Body.Close()
	}
	task.expire()
	select {
	case <-task.finished:
	case <-ctx.Done():
		t.Fatal("producer failed to join")
	}
	waitAdmissionCredits(t, k, baseline)
	if ackErr != nil || status != 200 || !strings.Contains(string(body), `"status":"completed"`) {
		t.Fatalf("real terminal-before-EOF cancel acknowledgement failed: status=%d bytes=%d err=%v", status, len(body), ackErr)
	}
	t.Logf("real background producer terminal-before-EOF cancel returned %d bytes with HTTP%d", len(body), status)
}

func TestFinalJSONCancelAcknowledgementPhases(t *testing.T) {
	for _, tc := range []struct {
		name           string
		complete, done bool
		want           string
	}{
		{"in_progress", false, false, `"status":"cancelled"`},
		{"producer_finished", true, true, `"status":"completed"`},
		{"terminal_recorded_producer_still_open", true, false, `"status":"completed"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{}`) }))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
			baseline := admissionSnapshot(k)
			f, err := k.newSpool()
			if err != nil {
				t.Fatal(err)
			}
			state, err := k.work.state(taskStateBytes(nil), &k.work.activeTasks)
			if err != nil {
				t.Fatal(err)
			}
			_, stop := context.WithCancel(context.Background())
			readStop, stopReads := context.WithCancel(context.Background())
			finished := make(chan struct{})
			close(finished)
			task := &responseTask{keeper: k, state: state, id: keeperResponsePrefix + "cancel_ack", cancel: stop, log: f, changed: make(chan struct{}), ready: make(chan struct{}), finished: finished, stream: true, status: 200, done: tc.done, readStop: readStop, stopReads: stopReads}
			defer task.expire()
			if tc.complete {
				raw := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_up","object":"response","status":"completed","output":[{"text":%q}]}}`, strings.Repeat("x", 128<<10)))
				if err := task.record(context.Background(), raw, nil); err != nil {
					t.Fatal(err)
				}
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/responses/"+task.id+"/cancel", nil)
			task.owner = k.ownerOf(req)
			k.cacheMu.Lock()
			k.tasks[task.id] = task
			k.cacheMu.Unlock()
			// A previous follow() has created readStop. Terminal record may exist while
			// serveHTTP still waits for upstream EOF; done remains false in production.
			resp, err := srv.Client().Post(srv.URL+req.URL.Path, "application/json", nil)
			var body []byte
			status := 0
			if err == nil {
				status = resp.StatusCode
				body, err = io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			task.expire()
			waitAdmissionCredits(t, k, baseline)
			if err != nil || status != 200 || !strings.Contains(string(body), tc.want) {
				t.Fatalf("cancel acknowledgement interrupted itself: status=%d bytes=%d err=%v", status, len(body), err)
			}
			t.Logf("cancel response remains readable: status=%d bytes=%d", status, len(body))
		})
	}
}

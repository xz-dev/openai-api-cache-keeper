package main

import (
	"context"
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

func TestBackgroundHTTPResponsesStayReal(t *testing.T) {
	for _, tc := range []struct {
		status int
		body   string
	}{
		{200, `{"id":"resp_nonstream","object":"response","status":"completed","store":true,"unknown":{"raw":true},"output":[]}`},
		{200, `{"id":"business_entity","opaque":[1,null]}`},
		{401, `{"error":{"code":"real_upstream_failure"}}`},
		{403, `{"error":{"code":"real_upstream_failure"}}`},
		{429, `{"error":{"code":"real_upstream_failure"}}`},
		{503, `{"error":{"code":"real_upstream_failure"}}`},
	} {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			status, want := tc.status, tc.body
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("Retry-After", "7")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, want)
			}))
			t.Cleanup(up.Close)
			u, _ := url.Parse(up.URL)
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
			got, body, header := post(t, srv, "Bearer owner", `{"model":"m","input":"A","background":true,"stream":true}`)
			if got != status || body != want || header.Get("Retry-After") != "7" {
				t.Fatalf("status=%d headers=%v body=%s", got, header, body)
			}
			k.cacheMu.Lock()
			tasks, bytes := len(k.tasks), k.eventBytes
			k.cacheMu.Unlock()
			if tasks != 0 || bytes != 0 {
				t.Fatalf("HTTP error cached: tasks=%d bytes=%d", tasks, bytes)
			}
		})
	}
}

func TestBackgroundDisconnectBeforeIdentity(t *testing.T) {
	for _, headers := range []bool{false, true} {
		t.Run(fmt.Sprintf("accepted=%t", headers), func(t *testing.T) {
			var creates atomic.Int32
			started, stopped := make(chan struct{}), make(chan struct{})
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				creates.Add(1)
				_, _ = io.Copy(io.Discard, r.Body)
				if headers {
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
				}
				close(started)
				<-r.Context().Done()
				close(stopped)
			}))
			t.Cleanup(up.Close)
			u, _ := url.Parse(up.URL)
			const ttl = 100 * time.Millisecond
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u; c.ttl = ttl })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"model":"m","input":"A","background":true,"stream":true}`))
			req.Header.Set("Authorization", "Bearer owner")
			result := make(chan error, 1)
			go func() {
				resp, err := http.DefaultClient.Do(req)
				if resp != nil {
					resp.Body.Close()
				}
				result <- err
			}()
			select {
			case <-started:
			case <-time.After(2 * time.Second):
				t.Fatal("upstream not started")
			}
			k.cacheMu.Lock()
			var task *responseTask
			for _, pending := range k.tasks {
				task = pending
			}
			k.cacheMu.Unlock()
			if task == nil {
				t.Fatal("no pending task")
			}
			if headers {
				deadline := time.After(2 * time.Second)
				for {
					task.mu.Lock()
					accepted, changed := task.stream, task.changed
					task.mu.Unlock()
					if accepted {
						break
					}
					select {
					case <-changed:
					case <-deadline:
						t.Fatal("keeper did not accept headers")
					}
				}
			}
			start := time.Now()
			cancel()
			select {
			case err := <-result:
				if err == nil {
					t.Error("cancelled request reported success without an identity")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("client did not stop")
			}
			select {
			case <-stopped:
			case <-time.After(2 * time.Second):
				t.Fatal("orphaned upstream did not stop")
			}
			if headers && time.Since(start) < ttl/2 {
				t.Error("accepted task cancelled before orphan window")
			}
			select {
			case <-task.finished:
			case <-time.After(2 * time.Second):
				t.Fatal("task did not finish")
			}
			if creates.Load() != 1 {
				t.Fatalf("hidden generations=%d", creates.Load())
			}
			k.cacheMu.Lock()
			pending, retained := len(k.tasks), k.eventBytes
			k.cacheMu.Unlock()
			if pending != 0 || retained != 0 {
				t.Fatalf("identity-less cancelled task retained: tasks=%d bytes=%d", pending, retained)
			}
		})
	}
}

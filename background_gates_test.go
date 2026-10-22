package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// gateUpstream streams chunks for up to limit generations, then rejects more.
func gateUpstream(t *testing.T, creates *atomic.Int32, limit int32, chunks func(w io.Writer, flush func())) *url.URL {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sub2api/billing" {
			_, _ = io.WriteString(w, `{}`)
			return
		}
		if creates.Add(1) > limit {
			http.Error(w, "unexpected generation", http.StatusInternalServerError)
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		chunks(w, w.(http.Flusher).Flush)
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	return u
}

func gateEvent(kind, extra string) string {
	if strings.HasPrefix(kind, "response.created") || kind == "response.completed" {
		status := "in_progress"
		if kind == "response.completed" {
			status = "completed"
		}
		return fmt.Sprintf("data: {\"type\":%q,\"response\":{\"id\":\"resp_up\",\"status\":%q,\"output\":[]}}\n\n", kind, status)
	}
	return fmt.Sprintf("data: {\"type\":%q,\"delta\":%q}\n\n", kind, extra)
}

func gateGet(t *testing.T, srv *httptest.Server, id, suffix string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/responses/"+id+suffix, nil)
	req.Header.Set("Authorization", "Bearer owner")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func gateSequences(t *testing.T, body string) ([]string, []int) {
	t.Helper()
	var kinds []string
	var seqs []int
	if err := readSSE(strings.NewReader(body), nil, func(data []byte) error {
		var e struct {
			Type     string `json:"type"`
			Sequence int    `json:"sequence_number"`
		}
		if err := json.Unmarshal(data, &e); err != nil {
			return err
		}
		kinds, seqs = append(kinds, e.Type), append(seqs, e.Sequence)
		return nil
	}); err != nil {
		t.Fatalf("replay is not valid SSE: %v: %q", err, body)
	}
	return kinds, seqs
}

func gateTask(t *testing.T, k *keeper, id string) *responseTask {
	t.Helper()
	k.cacheMu.Lock()
	task := k.tasks[id]
	k.cacheMu.Unlock()
	if task == nil {
		t.Fatal("missing task")
	}
	select {
	case <-task.finished:
	case <-time.After(3 * time.Second):
		t.Fatal("task did not finish")
	}
	return task
}

// Heartbeat comments and empty frames never advance the cursor; out-of-range
// cursors fail explicitly; a duplicate terminal is accepted at most once.
func TestBackgroundCursorAndTerminalIntegrity(t *testing.T) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 1, func(w io.Writer, flush func()) {
		for _, chunk := range []string{
			gateEvent("response.created", ""), ": keepalive\n\n", "event: ping\n\n",
			gateEvent("response.output_text.delta", "a"), ": keepalive 2\n\n",
			gateEvent("response.output_text.delta", "b"),
			gateEvent("response.completed", ""), gateEvent("response.completed", ""),
		} {
			_, _ = io.WriteString(w, chunk)
			flush()
		}
	})
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
	resp.Body.Close()
	gateTask(t, k, id)

	status, body := gateGet(t, srv, id, "?stream=true")
	kinds, seqs := gateSequences(t, body)
	want := "response.created,response.output_text.delta,response.output_text.delta,response.completed"
	if status != 200 || strings.Join(kinds, ",") != want || fmt.Sprint(seqs) != "[0 1 2 3]" {
		t.Fatalf("full replay status=%d kinds=%v seqs=%v", status, kinds, seqs)
	}
	status, body = gateGet(t, srv, id, "?stream=true&starting_after=1")
	if kinds, seqs = gateSequences(t, body); status != 200 || fmt.Sprint(seqs) != "[2 3]" || kinds[1] != "response.completed" {
		t.Fatalf("resume after heartbeat status=%d kinds=%v seqs=%v", status, kinds, seqs)
	}
	status, body = gateGet(t, srv, id, "?stream=true&starting_after=3")
	if kinds, _ = gateSequences(t, body); status != 200 || len(kinds) != 0 {
		t.Fatalf("resume after terminal status=%d body=%q", status, body)
	}
	for _, cursor := range []string{"4", "99", "-1", "x", "1.5"} {
		if status, body := gateGet(t, srv, id, "?stream=true&starting_after="+cursor); status != 400 {
			t.Errorf("cursor %s status=%d body=%s", cursor, status, body)
		}
	}
	if creates.Load() != 1 {
		t.Fatalf("hidden generations=%d", creates.Load())
	}
}

// An upstream frame cut mid-JSON is never recorded or acknowledged; the task
// fails explicitly and is not regenerated.
func TestBackgroundUpstreamPartialFrame(t *testing.T) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 1, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		_, _ = io.WriteString(w, gateEvent("response.output_text.delta", "a"))
		_, _ = io.WriteString(w, `data: {"type":"response.output_text.delta","del`)
	})
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
	resp.Body.Close()
	task := gateTask(t, k, id)
	task.mu.Lock()
	count, failed := task.count, task.err != nil
	task.mu.Unlock()
	if count != 2 || !failed {
		t.Fatalf("partial frame advanced cursor or succeeded: count=%d failed=%t", count, failed)
	}
	if status, body := gateGet(t, srv, id, "?stream=true&starting_after=0"); status != 409 {
		t.Fatalf("partial replay status=%d body=%s", status, body)
	}
	if creates.Load() != 1 {
		t.Fatalf("hidden generations=%d", creates.Load())
	}
}

// Readers share one event log: concurrent readers see identical events, a
// stalled reader does not stop generation, and log bytes are not multiplied.
func TestBackgroundMultipleAndSlowReaders(t *testing.T) {
	var creates atomic.Int32
	release := make(chan struct{})
	chunk := strings.Repeat("x", 256<<10)
	u := gateUpstream(t, &creates, 1, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		flush()
		<-release
		for range 8 {
			_, _ = io.WriteString(w, gateEvent("response.output_text.delta", chunk))
			flush()
		}
		_, _ = io.WriteString(w, gateEvent("response.completed", ""))
	})
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u; c.maxStoreBytes = 32 << 20 })
	resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
	resp.Body.Close()

	// Stalled reader: subscribe and never read the body.
	stalledCtx, stopStalled := context.WithCancel(context.Background())
	defer stopStalled()
	stalledReq, _ := http.NewRequestWithContext(stalledCtx, http.MethodGet, srv.URL+"/v1/responses/"+id+"?stream=true&starting_after=0", nil)
	stalledReq.Header.Set("Authorization", "Bearer owner")
	stalled, err := http.DefaultClient.Do(stalledReq)
	if err != nil {
		t.Fatal(err)
	}
	defer stalled.Body.Close()

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i := range bodies {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, bodies[i] = gateGet(t, srv, id, "?stream=true")
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	task := gateTask(t, k, id)
	wg.Wait()
	if bodies[0] != bodies[1] {
		t.Fatal("concurrent readers saw different events")
	}
	kinds, _ := gateSequences(t, bodies[0])
	if len(kinds) != 10 || kinds[9] != "response.completed" {
		t.Fatalf("reader events=%d last=%v", len(kinds), kinds[len(kinds)-1:])
	}
	task.mu.Lock()
	size := task.log.size
	task.mu.Unlock()
	k.cacheMu.Lock()
	bytes := k.eventBytes
	k.cacheMu.Unlock()
	if bytes != size || bytes > int64(len(bodies[0]))+4096 {
		t.Fatalf("event log charged %d for one %d-byte log (%d-byte replay)", bytes, size, len(bodies[0]))
	}
	if creates.Load() != 1 {
		t.Fatalf("hidden generations=%d", creates.Load())
	}
}

// A completed task with no reader is released after TTL; a process restart
// loses in-flight/completed tasks, which fail explicitly without regeneration.
func TestBackgroundCompletedTTLAndRestart(t *testing.T) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 2, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		_, _ = io.WriteString(w, gateEvent("response.completed", ""))
	})
	dir := t.TempDir()
	const ttl = 150 * time.Millisecond
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u; c.ttl = ttl; c.storeDir = dir })
	resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
	resp.Body.Close()
	gateTask(t, k, id)
	if status, body := gateGet(t, srv, id, ""); status != 200 || !strings.Contains(body, `"completed"`) {
		t.Fatalf("completed read status=%d body=%s", status, body)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		k.cacheMu.Lock()
		tasks, bytes := len(k.tasks), k.eventBytes
		k.cacheMu.Unlock()
		if tasks == 0 && bytes == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("completed task not released after TTL: tasks=%d bytes=%d", tasks, bytes)
		}
		time.Sleep(20 * time.Millisecond)
	}
	if status, _ := gateGet(t, srv, id, "?stream=true&starting_after=0"); status != 404 {
		t.Fatalf("expired replay status=%d", status)
	}

	// Restart: a second process over the same disk has no task registry.
	resp2, id2 := openBackgroundTask(t, srv, `{"model":"m","input":"B","background":true,"stream":true}`)
	resp2.Body.Close()
	srv.Close()
	if err := k.store.close(); err != nil {
		t.Fatal(err)
	}
	cfg := k.cfg
	cfg.ttl = time.Minute
	restarted := newKeeper(cfg)
	t.Cleanup(func() { _ = restarted.store.close() })
	srv2 := httptest.NewServer(restarted)
	t.Cleanup(srv2.Close)
	for _, suffix := range []string{"", "?stream=true&starting_after=0"} {
		if status, body := gateGet(t, srv2, id2, suffix); status != 404 {
			t.Fatalf("restarted read %q status=%d body=%s", suffix, status, body)
		}
	}
	req, _ := http.NewRequest(http.MethodPost, srv2.URL+"/v1/responses/"+id2+"/cancel", nil)
	req.Header.Set("Authorization", "Bearer owner")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatalf("restarted cancel status=%d", res.StatusCode)
	}
	if n := creates.Load(); n != 2 {
		t.Fatalf("restart reads changed generations=%d", n)
	}
}

// Event log write failure invalidates the task; failed cleanup keeps bytes charged.
func TestBackgroundLogWriteAndCleanupFailure(t *testing.T) {
	for _, mode := range []string{"write", "cleanup"} {
		t.Run(mode, func(t *testing.T) {
			var creates atomic.Int32
			release := make(chan struct{})
			u := gateUpstream(t, &creates, 1, func(w io.Writer, flush func()) {
				_, _ = io.WriteString(w, gateEvent("response.created", ""))
				flush()
				<-release
				_, _ = io.WriteString(w, gateEvent("response.output_text.delta", "a"))
				_, _ = io.WriteString(w, gateEvent("response.completed", ""))
			})
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
			resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
			resp.Body.Close()
			k.cacheMu.Lock()
			task := k.tasks[id]
			k.cacheMu.Unlock()
			task.mu.Lock()
			name := task.log.Name()
			task.mu.Unlock()
			k.work.mu.Lock()
			base := k.work.disk
			k.work.mu.Unlock()
			fill := maxSpoolBytes - base
			if mode == "write" {
				k.work.mu.Lock()
				k.work.disk += fill
				k.work.mu.Unlock()
			} else {
				// Replace the owned file so removal is refused as unowned cleanup.
				if err := os.Rename(name, name+".moved"); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(name, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = os.Remove(name + ".moved") })
			}
			close(release)
			gateTask(t, k, id)
			if mode == "write" {
				if status, body := gateGet(t, srv, id, "?stream=true&starting_after=0"); status != 409 {
					t.Fatalf("failed log replay status=%d body=%s", status, body)
				}
				k.work.mu.Lock()
				k.work.disk -= fill
				disk := k.work.disk
				k.work.mu.Unlock()
				k.cacheMu.Lock()
				bytes := k.eventBytes
				k.cacheMu.Unlock()
				if disk != 0 || bytes != 0 {
					t.Fatalf("failed log leaked charges: disk=%d events=%d", disk, bytes)
				}
			} else {
				task.expire()
				// work.disk is the physical spool bound; eventBytes only budgets replayable logs.
				deadline := time.Now().Add(3 * time.Second)
				for {
					k.work.mu.Lock()
					disk := k.work.disk
					k.work.mu.Unlock()
					task.mu.Lock()
					released := task.log == nil
					task.mu.Unlock()
					if released && disk > 0 {
						break
					}
					if disk == 0 || time.Now().After(deadline) {
						t.Fatalf("refused cleanup pretended bytes were reclaimed: disk=%d released=%t", disk, released)
					}
					time.Sleep(10 * time.Millisecond)
				}
				if _, err := os.Stat(name + ".moved"); err != nil {
					t.Fatalf("unowned cleanup removed data: %v", err)
				}
				if status, _ := gateGet(t, srv, id, ""); status != 404 {
					t.Fatalf("expired task readable: %d", status)
				}
			}
			if creates.Load() != 1 {
				t.Fatalf("hidden generations=%d", creates.Load())
			}
		})
	}
}

// Cancel while a reader follows ends that reader promptly with no regeneration.
func TestBackgroundCancelEndsActiveReader(t *testing.T) {
	var creates atomic.Int32
	hold := make(chan struct{})
	defer close(hold)
	u := gateUpstream(t, &creates, 1, func(w io.Writer, flush func()) {
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		flush()
		<-hold
	})
	_, srv, _ := newTestKeeper(t, func(c *config) { c.upstream = u })
	resp, id := openBackgroundTask(t, srv, `{"model":"m","input":"A","background":true,"stream":true}`)
	resp.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/responses/"+id+"?stream=true&starting_after=0", nil)
	req.Header.Set("Authorization", "Bearer owner")
	reader, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Body.Close()
	done := make(chan error, 1)
	go func() { _, err := io.ReadAll(reader.Body); done <- err }()
	cancelReq, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses/"+id+"/cancel", nil)
	cancelReq.Header.Set("Authorization", "Bearer owner")
	res, err := http.DefaultClient.Do(cancelReq)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled reader ended as a clean stream")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reader not released by cancel")
	}
	if creates.Load() != 1 {
		t.Fatalf("hidden generations=%d", creates.Load())
	}
}

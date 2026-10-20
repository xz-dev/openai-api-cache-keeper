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
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func waitWorking(t *testing.T, k *keeper, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if len(k.work.turns) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("turn slots=%d want=%d", len(k.work.turns), want)
}
func TestQueuedWSPingAndCancellation(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	leave, err := k.work.enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	conn := dialWS(t, srv, "Bearer queued")
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	// A real queued message is on disk while phase remains unavailable. The
	// independent reader must keep processing pings, then notice socket close.
	if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","input":"queued"}`)); err != nil {
		t.Fatal(err)
	}
	waitWorking(t, k, 1)
	readDone := make(chan struct{})
	go func() { defer close(readDone); _, _, _ = conn.Read(ctx) }()
	for i := 0; i < 3; i++ {
		pingCtx, stop := context.WithTimeout(ctx, time.Second)
		err := conn.Ping(pingCtx)
		stop()
		if err != nil {
			t.Fatalf("queued ping: %v", err)
		}
	}
	conn.CloseNow()
	<-readDone
	waitWorking(t, k, 0)
	files, _ := filepath.Glob(spoolFilePattern(k.cfg.storeDir))
	if len(files) != 0 {
		t.Fatalf("queued cancel leaked files: %v", files)
	}
}
func TestQueuedHTTPCancellationAndOverflow(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	leave, err := k.work.enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "POST", srv.URL+"/v1/responses", strings.NewReader(`{"input":"queued"}`))
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			resp.Body.Close()
		}
	}()
	waitWorking(t, k, 1)
	cancel()
	<-done
	waitWorking(t, k, 0)
	for i := 0; i < maxTurns; i++ {
		k.work.turns <- struct{}{}
	}
	status, body, _ := post(t, srv, "Bearer overflow", `{"input":"rejected"}`)
	for i := 0; i < maxTurns; i++ {
		<-k.work.turns
	}
	if status != 503 || !strings.Contains(body, "working_set_unavailable") {
		t.Fatalf("overflow: %d %s", status, body)
	}
	files, _ := filepath.Glob(spoolFilePattern(k.cfg.storeDir))
	if len(files) != 0 {
		t.Fatal("cancel/overflow leaked spool")
	}
}
func TestRequestExactCeilingAndOversize(t *testing.T) {
	_, srv, k := newTestKeeper(t, func(c *config) { c.maxRequestBytes = 4096 })
	prefix := `{"input":"`
	suffix := `","store":false}`
	request := prefix + strings.Repeat("X", 4096-len(prefix)-len(suffix)) + suffix
	status, body, _ := post(t, srv, "Bearer size", request)
	if status != 200 || !strings.Contains(body, `"status":"completed"`) {
		t.Fatalf("exact ceiling: %d %s", status, body)
	}
	status, body, _ = post(t, srv, "Bearer size", request+" ")
	if status != 413 || !strings.Contains(body, "working_set_too_large") {
		t.Fatalf("oversize: %d %s", status, body)
	}
	waitWorking(t, k, 0)
}
func TestEncodedInputDoesNotGrowWithHTMLCharacters(t *testing.T) {
	_, _, k := newTestKeeper(t)
	text := strings.Repeat("<>&", 100000)
	raw, err := k.spoolRequest(context.Background(), strings.NewReader(`{"input":"`+text+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	leave, err := k.work.enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	turn, apiErr := k.prepareDisk(context.Background(), raw, [32]byte{1}, func(string) ([32]byte, bool) { return [32]byte{}, false }, false)
	if apiErr != nil {
		t.Fatal(apiErr)
	}
	defer turn.file.Close()
	if turn.file.size > int64(len(text)+100) {
		t.Fatalf("HTML escaping expanded payload: %d", turn.file.size)
	}
	full, err := turn.history()
	if err != nil {
		t.Fatal(err)
	}
	var item struct{ Content string }
	if len(full) != 1 || json.Unmarshal(full[0], &item) != nil || item.Content != text {
		t.Fatal("text changed")
	}
}
func TestExpandedHistoryBoundBeforeCopy(t *testing.T) {
	s := newTestStore(t, time.Hour, 2<<20, 1<<20, 1<<20, 4<<20)
	owner := [32]byte{1}
	raw, _ := json.Marshal(strings.Repeat("X", 1<<20))
	full := make([]json.RawMessage, 65)
	for i := range full {
		full[i] = raw
	}
	mustReviewPut(t, s, "large-expanded", owner, "", full)
	if _, ok, err := s.boundedGet("large-expanded", owner, false); ok || err != errWorkSize {
		t.Fatalf("dedup expansion escaped bound: ok=%v err=%v", ok, err)
	}
	// Refusal never deletes accepted reference or truncates its history.
	got, ok := s.get("large-expanded", owner)
	if !ok || len(got) != 65 {
		t.Fatal("refusal changed accepted history")
	}
}
func TestSpoolBudgetAndDiskErrorCleanup(t *testing.T) {
	_, _, k := newTestKeeper(t)
	k.work.mu.Lock()
	k.work.disk = maxSpoolBytes
	k.work.mu.Unlock()
	if f, err := k.spoolRequest(context.Background(), strings.NewReader("x")); err == nil {
		f.Close()
		t.Fatal("disk budget bypass")
	}
	k.work.mu.Lock()
	k.work.disk = 0
	k.work.mu.Unlock()
	files, _ := filepath.Glob(spoolFilePattern(k.cfg.storeDir))
	if len(files) != 0 {
		t.Fatal("failed spool leaked file")
	}
	old := k.cfg.storeDir
	k.cfg.storeDir = filepath.Join(old, "missing")
	if _, err := k.spoolRequest(context.Background(), strings.NewReader("x")); err == nil {
		t.Fatal("expected real filesystem create failure")
	}
	k.cfg.storeDir = old
}
func TestOutputBoundsAndIncompleteStreamExplicit(t *testing.T) {
	for _, mode := range []string{"event", "body", "incomplete"} {
		t.Run(mode, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "body" {
					w.Header().Set("Content-Type", "application/json")
					io.WriteString(w, strings.Repeat("X", maxResponseBytes+1))
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				if mode == "event" {
					fmt.Fprintf(w, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"%s\"}\n\n", strings.Repeat("X", maxEventBytes))
					return
				}
				io.WriteString(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_never\",\"status\":\"in_progress\"}}\n\n")
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
			status, body, _ := post(t, srv, "Bearer output", `{"input":"A","stream":true}`)
			if strings.Contains(body, `"status":"completed"`) || (!strings.Contains(body, "working_set_too_large") && !strings.Contains(body, "upstream_error")) {
				t.Fatalf("missing explicit failure: %d %s", status, body[:min(len(body), 500)])
			}
			if n, _ := k.store.stats(); n != 0 {
				t.Fatal("failed output stored")
			}
			c := dialWS(t, srv, "Bearer output")
			ev := wsCreate(t, c, `{"type":"response.create","input":"A"}`)
			if ev["type"] != "error" {
				t.Fatalf("WS false terminal: %v", ev)
			}
		})
	}
}
func TestSpoolRemovedAfterSuccessfulTurns(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	for i := 0; i < 5; i++ {
		status, _, _ := post(t, srv, "Bearer cleanup", `{"input":"A","stream":true}`)
		if status != 200 {
			t.Fatal(status)
		}
	}
	waitWorking(t, k, 0)
	files, err := os.ReadDir(filepath.Join(k.cfg.storeDir, spoolDirName))
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if strings.HasPrefix(f.Name(), "turn-") {
			t.Fatal("success leaked spool")
		}
	}
	k.work.mu.Lock()
	disk, output := k.work.disk, k.work.output
	k.work.mu.Unlock()
	if disk != 0 || output != 0 {
		t.Fatalf("charges leaked: disk=%d output=%d", disk, output)
	}
}

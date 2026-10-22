package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestResponseMetadataRejectsBeforeGrowth(t *testing.T) {
	raw := []byte(`{"id":"resp_many","status":"completed","output":[` + strings.Repeat(`{},`, 199999) + `{}]}`)
	m := newWorkingSet()
	var c collector
	defer c.release(m)
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := c.boundedResponse(raw, m)
	runtime.ReadMemStats(&after)
	if err != errWorkSize || len(c.output) != 0 {
		t.Fatalf("metadata admitted: %v / %d", err, len(c.output))
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 8<<20 {
		t.Fatalf("allocation before refusal=%d", alloc)
	}
	// Top-level fields have an independent cap before map allocation.
	fields := []byte(`{"status":"completed","id":"resp_fields"` + strings.Repeat(`,"x":0`, 10000) + `}`)
	if err := c.boundedResponse(fields, m); err != errWorkSize {
		t.Fatalf("field cap bypass: %v", err)
	}
}
func TestFlatSSECommentsChargedWithoutLineAllocations(t *testing.T) {
	body := strings.Repeat(":\n", 200000) + "\n"
	m := newWorkingSet()
	frames := 0
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	err := readSSE(strings.NewReader(body), func(frame []byte) error {
		frames++
		if string(frame) != body {
			t.Fatal("raw comment event altered")
		}
		m.mu.Lock()
		charge := m.output
		m.mu.Unlock()
		if charge < int64(len(body)) {
			t.Fatalf("raw event uncharged: %d", charge)
		}
		return nil
	}, func([]byte) error { t.Fatal("comments became data"); return nil }, m)
	runtime.ReadMemStats(&after)
	if err != nil || frames != 1 {
		t.Fatalf("frames=%d err=%v", frames, err)
	}
	if alloc := after.TotalAlloc - before.TotalAlloc; alloc > 4<<20 {
		t.Fatalf("per-line allocation amplification=%d", alloc)
	}
	if m.output != 0 {
		t.Fatalf("raw frame leaked charge=%d", m.output)
	}
}
func TestConcurrentStoreFalseMetadataRefusal(t *testing.T) {
	raw := `{"object":"response","id":"resp_many","status":"completed","output":[` + strings.Repeat(`{},`, 199999) + `{}]}`
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, raw)
	}))
	defer upstream.Close()
	u, _ := url.Parse(upstream.URL)
	_, srv, k := newLocalTestKeeper(t, func(c *config) { c.upstream = u })
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Go(func() {
			status, body, _ := post(t, srv, "Bearer metadata", `{"input":"A","store":false}`)
			if status == 200 || !strings.Contains(body, "working_set") {
				t.Errorf("store:false bypass: %d %s", status, body[:min(200, len(body))])
			}
		})
	}
	wg.Wait()
	waitWorking(t, k, 0)
	k.work.mu.Lock()
	charge := k.work.output
	k.work.mu.Unlock()
	if charge != 0 {
		t.Fatalf("output charge leaked=%d", charge)
	}
	if entries, _ := k.store.stats(); entries != 0 {
		t.Fatal("rejected metadata stored")
	}
}
func TestEscapedRewriteBudgetAndPhaseLifetime(t *testing.T) {
	_, _, k := newTestKeeper(t)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			var c collector
			defer c.release(k.work)
			text := strings.Repeat("<>&", 16000)
			raw := []byte(`{"object":"response","id":"msg_escape","status":"completed","output":[{"text":"` + text + `"}]}`)
			wire, release, err := k.responseFrame(context.Background(), &c, raw, false, true, nil)
			if err != nil {
				t.Errorf("bounded escaped response: %v", err)
				return
			}
			defer release()
			if len(wire) > len(raw)+64 {
				t.Errorf("HTML escaping amplification %d -> %d", len(raw), len(wire))
			}
			var doc map[string]json.RawMessage
			if json.Unmarshal(wire, &doc) != nil {
				t.Error("wire invalid")
			}
			k.work.mu.Lock()
			charge := k.work.output
			k.work.mu.Unlock()
			if charge > maxCollectedBytes {
				t.Error("global response budget exceeded")
			}
			// Caller can own a wire buffer (or block writing it) without owning phase.
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			leave, err := k.work.enter(ctx)
			if err != nil {
				t.Error("response phase retained across writer")
				return
			}
			leave()
		})
	}
	wg.Wait()
	if k.work.output != 0 {
		t.Fatalf("response reservations leaked=%d", k.work.output)
	}
}
func TestTerminalJSONValidationAndAsynchronousPassThrough(t *testing.T) {
	for _, tc := range []struct {
		body    string
		success bool
		stored  bool
	}{
		{`{"object":"response","id":"resp_x","output":[]}`, false, false},
		{`{"id":"resp_x","status":"completed","output":{}}`, false, false},
		{`{"id":"resp_x","status":"incomplete","output":[]}`, true, true},
		{`{"object":"response","id":"resp_x","status":"queued","output":[]}`, true, false},
		{`{"object":"response","id":"resp_x","status":"in_progress","output":[]}`, true, false},
	} {
		t.Run(tc.body, func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "application/json")
				io.WriteString(w, tc.body)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newLocalTestKeeper(t, func(c *config) { c.upstream = u })
			status, body, _ := post(t, srv, "Bearer terminal", `{"input":"A"}`)
			if (status == 200) != tc.success {
				t.Fatalf("status=%d body=%s", status, body)
			}
			if !tc.success && !strings.Contains(body, "upstream_error") {
				t.Fatal("missing upstream validation error")
			}
			entries, _ := k.store.stats()
			if (entries == 1) != tc.stored {
				t.Fatalf("entries=%d", entries)
			}
		})
	}
}
func TestSSEDelimiterAndReadFailure(t *testing.T) {
	terminal := `data: {"type":"response.incomplete","response":{"id":"resp_x","status":"incomplete","output":[]}}`
	for _, ending := range []string{"", "\n", "\n\n"} {
		count := 0
		err := readSSE(strings.NewReader(terminal+ending), nil, func(raw []byte) error {
			var c collector
			if err := c.event(raw); err != nil {
				return err
			}
			count++
			return nil
		})
		if ending == "\n\n" {
			if err != nil || count != 1 {
				t.Fatalf("complete incomplete terminal rejected: %v", err)
			}
		} else if err == nil || count != 0 {
			t.Fatalf("unterminated event accepted: %q %v %d", ending, err, count)
		}
	}
	count := 0
	err := readSSE(io.MultiReader(strings.NewReader(terminal+"\n\n"), finalReadError{}), nil, func([]byte) error { count++; return nil })
	if err != io.ErrUnexpectedEOF || count != 1 {
		t.Fatalf("already delimited event lost: %v %d", err, count)
	}
}
func TestTruncatedWSTurnPreservesAcceptedPin(t *testing.T) {
	fake := &fakeUpstream{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if bytes.Contains(body, []byte("BAD-TURN")) {
			raw := `data: {"type":"response.completed","response":{"id":"resp_bad","status":"completed","output":[]}}`
			w.Header().Set("Content-Type", "text/event-stream")
			w.Header().Set("Content-Length", fmt.Sprint(len(raw)+10))
			io.WriteString(w, raw)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		fake.ServeHTTP(w, r)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, k := newLocalTestKeeper(t, func(c *config) { c.upstream = u })
	conn := dialWS(t, srv, "Bearer private")
	first := wsCreate(t, conn, `{"type":"response.create","input":"A","store":false}`)
	id := responseID(first)
	failed := wsCreate(t, conn, fmt.Sprintf(`{"type":"response.create","input":"BAD-TURN","previous_response_id":%q}`, id))
	if _, ok := failureCode(failed); !ok {
		t.Fatalf("truncated turn succeeded: %v", failed)
	}
	next := wsCreate(t, conn, fmt.Sprintf(`{"type":"response.create","input":"C","store":false,"previous_response_id":%q}`, id))
	if next["type"] != "response.completed" || len(fake.last()) != 4 {
		t.Fatal("accepted private history replaced by failed turn")
	}
	if n, _ := k.store.stats(); n != 0 {
		t.Fatal("truncated turn published alias")
	}
	inspectStore(t, k.store)
}
func TestOwnedSpoolCleanupPreservesForeignEntries(t *testing.T) {
	dir := t.TempDir()
	k := &keeper{cfg: config{storeDir: dir}, work: newWorkingSet()}
	f, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	owned := f.Name()
	f.Write([]byte("private"))
	f.File.Close()
	ns := filepath.Join(dir, spoolDirName)
	foreign := []string{filepath.Join(dir, "turn-foreign"), filepath.Join(ns, "turn-foreign"), filepath.Join(ns, "turn-link"), filepath.Join(ns, "turn-dir")}
	for _, name := range foreign[:2] {
		if err := os.WriteFile(name, []byte("foreign"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	target := filepath.Join(t.TempDir(), "outside")
	os.WriteFile(target, []byte("outside"), 0600)
	if err := os.Symlink(target, foreign[2]); err != nil {
		t.Fatal(err)
	}
	os.Mkdir(foreign[3], 0700)
	if err := cleanupTransient(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(owned); !os.IsNotExist(err) {
		t.Fatal("owned stale spool survived")
	}
	for _, name := range foreign {
		if _, err := os.Lstat(name); err != nil {
			t.Fatalf("foreign entry deleted: %s", name)
		}
	}
	if data, err := os.ReadFile(target); err != nil || string(data) != "outside" {
		t.Fatal("symlink target touched")
	}
}
func TestSpoolBodyOwnsChargeUntilClose(t *testing.T) {
	_, _, k := newTestKeeper(t)
	f, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte("payload"))
	name := f.Name()
	body, err := f.requestBody()
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if k.work.disk != f.offset+7 {
		t.Fatalf("header/payload uncharged while transport owns file: %d", k.work.disk)
	}
	data, err := io.ReadAll(body)
	if err != nil || string(data) != "payload" {
		t.Fatalf("body after owner close: %q %v", data, err)
	}
	if err := body.Close(); err != nil {
		t.Fatal(err)
	}
	body.Close()
	f.Close()
	if k.work.disk != 0 {
		t.Fatal("body close charge leaked")
	}
	if _, err := os.Lstat(name); !os.IsNotExist(err) {
		t.Fatal("body close leaked spool")
	}
}

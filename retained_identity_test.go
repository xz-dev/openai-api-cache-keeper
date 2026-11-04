package main

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestRetainedTaskIdentityChargedOnce(t *testing.T) {
	for _, nativeParent := range []bool{false, true} {
		t.Run(fmt.Sprint(nativeParent), func(t *testing.T) {
			id := "resp_" + strings.Repeat("x", 256<<10)
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.Copy(io.Discard, r.Body)
				w.Header().Set("Content-Type", "text/event-stream")
				for i := 0; i < 3; i++ {
					fmt.Fprintf(w, "data: {\"type\":\"response.created\",\"response\":{\"id\":%q,\"status\":\"in_progress\",\"output\":[]}}\n\n", id)
				}
				fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":%q,\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\n", id)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			// The native-parent log genuinely stores each upstream event's 256KiB id
			// (4x262302 > the 1MiB test store budget; STORE_MAX_BYTES defaults to 0=unlimited).
			// This is the store/log retention bound, not the 16MiB output or 8MiB
			// response limit.
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u; c.maxStoreBytes = 4 << 20 })
			baseline := admissionSnapshot(k)
			body := `{"input":"x","store":false,"background":true,"stream":true}`
			if nativeParent {
				body = `{"input":"x","store":false,"background":true,"stream":true,"previous_response_id":"upstream_parent"}`
			}
			res, err := srv.Client().Post(srv.URL+"/v1/responses", "application/json", strings.NewReader(body))
			if err != nil {
				t.Fatal(err)
			}
			raw, err := io.ReadAll(res.Body)
			res.Body.Close()
			if err != nil || res.StatusCode != 200 || !strings.Contains(string(raw), "response.completed") {
				t.Fatalf("response=%d bytes=%d err=%v", res.StatusCode, len(raw), err)
			}
			k.cacheMu.Lock()
			var task *responseTask
			for _, x := range k.tasks {
				task = x
			}
			k.cacheMu.Unlock()
			if task == nil {
				t.Fatal("completed task not retained")
			}
			select {
			case <-task.finished:
			case <-time.After(3 * time.Second):
				t.Fatal("producer did not finish")
			}
			deadline := time.Now().Add(3 * time.Second)
			for k.work.activeTurns.Load() != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			task.mu.Lock()
			retained := len(task.upstreamID)
			if task.upstreamID != id || task.nativeParent != nativeParent {
				t.Error("retained identity or native-parent mode changed")
			}
			if nativeParent {
				if task.id != id {
					t.Error("native public task id changed")
				}
				retained += len(task.id)
			}
			task.mu.Unlock()
			got := admissionSnapshot(k)
			if got.input-baseline.input < int64(retained) || got.output != baseline.output {
				t.Fatalf("retained=%d idle credits=%+v baseline=%+v", retained, got, baseline)
			}
			task.state.mu.Lock()
			extras := len(task.state.extras)
			task.state.mu.Unlock()
			if extras > 3 {
				t.Fatalf("repeated same-id events accumulated %d ownership extensions", extras)
			}
			task.expire()
			waitAdmissionCredits(t, k, baseline)
		})
	}
}

func TestRetainedSessionIdentityReplacementAndPressure(t *testing.T) {
	m := newWorkingSet()
	s := &wsSession{}
	input := func() int64 {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.input
	}
	install := func(id string) {
		t.Helper()
		free, err := s.reserveID(m, id)
		if err != nil {
			t.Fatal(err)
		}
		s.setID(id, free)
	}
	large := "resp_" + strings.Repeat("x", 256<<10)
	install(large)
	if input() != int64(len(large)) {
		t.Fatal("initial identity does not own its exact bytes")
	}
	for i := 0; i < 32; i++ {
		install(strings.Clone(large))
	}
	if input() != int64(len(large)) {
		t.Fatal("same identity accumulated credit")
	}
	// Genuine used+requested pressure; the prior identity must survive refusal.
	hold, err := m.reserveInput((80 << 20) - input())
	if err != nil {
		t.Fatal(err)
	}
	free, err := s.reserveID(m, "resp_small")
	if !errors.Is(err, errWorkSize) || free != nil || s.lastID != large {
		t.Fatalf("pressure did not preserve prior identity: err=%v", err)
	}
	install(large) // No new ownership is needed for the same value, even at capacity.
	hold()
	// A failed store transaction drops only the proposed identity's credit.
	free, err = s.reserveID(m, "resp_uncommitted")
	if err != nil {
		t.Fatal(err)
	}
	free()
	if s.lastID != large || input() != int64(len(large)) {
		t.Fatal("aborted replacement changed the prior identity or its credit")
	}
	install("resp_small")
	if input() != int64(len("resp_small")) {
		t.Fatalf("old identity credit remains after replacement: %d", input())
	}
	s.clear()
	s.clear()
	if input() != 0 || s.lastID != "" || s.idFree != nil {
		t.Fatal("session clear leaked identity ownership")
	}
}

func TestRetainedLargeIdentityAcrossBothWebSocketPaths(t *testing.T) {
	for _, local := range []bool{true, false} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			large := "resp_" + strings.Repeat("z", 256<<10)
			var turns atomic.Int64
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				next := func() string {
					if turns.Add(1) == 1 {
						return large
					}
					return "resp_small"
				}
				event := func(id string) string {
					return fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"object":"response","status":"completed","store":false,"output":[]}}`, id)
				}
				if !isUpgradeRequest(r) {
					_, _ = io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "data: %s\n\n", event(next()))
					return
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				for {
					if _, _, err := conn.Read(r.Context()); err != nil {
						return
					}
					if err := conn.Write(r.Context(), websocket.MessageText, []byte(event(next()))); err != nil {
						return
					}
				}
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newKeeperFixture(t, local, func(c *config) { c.upstream = u })
			baseline := admissionSnapshot(k)
			conn := dialWS(t, srv, "Bearer a")
			conn.SetReadLimit(1 << 20)
			ev := wsCreate(t, conn, `{"type":"response.create","model":"m","input":"A","store":false}`)
			if responseID(ev) != large {
				t.Fatal("large identity was truncated or rejected")
			}
			deadline := time.Now().Add(3 * time.Second)
			for admissionSnapshot(k).output != 0 && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			first := admissionSnapshot(k)
			if first.input-baseline.input < int64(len(large)) || first.output != 0 {
				t.Fatalf("idle large identity has no persistent input ownership: %+v", first)
			}
			ev = wsCreate(t, conn, `{"type":"response.create","model":"m","input":"B","store":false,"previous_response_id":"`+large+`"}`)
			if responseID(ev) != "resp_small" {
				t.Fatal("same-socket continuation or replacement failed")
			}
			conn.CloseNow()
			waitAdmissionCredits(t, k, baseline)
		})
	}
}

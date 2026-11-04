package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// Observe the real serializer, not a timer standing in for queued acceptance.
func waitQueuedResponse(t *testing.T) {
	t.Helper()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		stacks := make([]byte, 128<<10)
		n := runtime.Stack(stacks, true)
		if strings.Contains(string(stacks[:n]), "(*store).acquireMutation") {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("response did not reach actual mutation queue")
}

func keeperFDs(t *testing.T, k *keeper) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", entry.Name()))
		if err == nil && strings.HasPrefix(target, k.cfg.storeDir+string(os.PathSeparator)) {
			count++
		}
	}
	return count
}

func assertQueuedCleanup(t *testing.T, k *keeper, baseline admissionCredits, fds int, gen *storeGeneration) {
	t.Helper()
	waitAdmissionCredits(t, k, baseline)
	if got := keeperFDs(t, k); got != fds {
		t.Errorf("owned descriptors=%d want=%d", got, fds)
	}
	k.store.generationMu.Lock()
	defer k.store.generationMu.Unlock()
	if k.store.current != gen {
		t.Error("unexpected generation replacement")
	}
	for _, g := range k.store.generations {
		if g.refs != 0 {
			t.Errorf("generation references=%d", g.refs)
		}
	}
}

// Replaces the former map-held-during-accept oracle with actual immutable
// pending ownership, a real store queue/commit, and terminal-map reuse.
func TestParsedEventPendingStoreOwnership(t *testing.T) {
	for _, outcome := range []string{"commit", "cancel-queued", "cancel-staged"} {
		t.Run(outcome, func(t *testing.T) {
			_, _, k := newTestKeeper(t)
			baseline, fds, gen := admissionSnapshot(k), keeperFDs(t, k), k.store.current
			input, err := k.spoolRequest(context.Background(), strings.NewReader(`{"input":"tiny","store":true}`), nil)
			if err != nil {
				t.Fatal(err)
			}
			turn, apiErr := k.prepareDiskSnapshot(context.Background(), input, [32]byte{}, nil, false, false)
			input.Close()
			if apiErr != nil {
				t.Fatal(apiErr)
			}
			defer turn.file.Close()
			raw := []byte(`{"type":"response.completed","response":{"id":"resp_pending","status":"completed","store":true,"output":["result"]},"opaque":"` + strings.Repeat("x", 512<<10) + `"}`)
			body, err := readResponseBody(bytes.NewReader(raw), k.work)
			if err != nil {
				t.Fatal(err)
			}
			defer body.close()
			var c collector
			defer c.release(k.work)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered, stage, releaseStage := make(chan struct{}), make(chan struct{}), make(chan struct{})
			if outcome == "cancel-staged" {
				k.store.stageCommitted = func() error { close(stage); <-releaseStage; return nil }
			} else {
				k.store.mu.Lock()
			}
			type result struct {
				wire   []byte
				free   func()
				parsed map[string]json.RawMessage
				err    error
			}
			done := make(chan result, 1)
			go func() {
				prepare := func() (*responseAcceptance, error) {
					rid, output, ok := c.final()
					if !ok {
						return nil, errors.New("missing final")
					}
					pending, err := k.prepareAcceptDisk(ctx, turn, rid, [32]byte{}, output, "", false)
					if err != nil {
						return nil, err
					}
					return &responseAcceptance{close: pending.close, commit: func() (bool, error) { close(entered); _, err := pending.commit(ctx); return err == nil, err }}, nil
				}
				wire, free, parsed, err := k.responseFrameOwned(ctx, &c, body, true, true, prepare, nil)
				done <- result{wire, free, parsed, err}
			}()
			<-entered
			if outcome == "cancel-staged" {
				<-stage
			} else {
				waitQueuedResponse(t)
			}
			held := admissionSnapshot(k)
			if held.output != 0 {
				t.Errorf("queued response output=%d", held.output)
			}
			if held.input-baseline.input < int64(len(raw)) {
				t.Error("pending source is not byte-precharged")
			}
			if c.charged != 0 || c.items != nil || c.output != nil || c.pendingState == nil || body.data != nil {
				t.Error("live collector/raw output survived handoff")
			}
			if outcome != "commit" {
				cancel()
			}
			if outcome == "cancel-staged" {
				close(releaseStage)
			} else {
				k.store.mu.Unlock()
			}
			out := <-done
			if outcome == "commit" {
				if out.err != nil || !bytes.Equal(out.wire, raw) || out.parsed == nil {
					t.Errorf("unchanged terminal wire/map lost: err=%v", out.err)
				}
				history, ok := k.store.get("resp_pending", [32]byte{})
				if !ok || len(history) != 2 || string(history[1]) != `"result"` {
					t.Errorf("real pending history missing: %v", history)
				}
			} else {
				if !errors.Is(out.err, context.Canceled) {
					t.Errorf("cancel err=%v", out.err)
				}
				if _, _, ok := k.store.resolveID("resp_pending", [32]byte{}, false); ok {
					t.Error("cancelled history published")
				}
			}
			out.wire, out.parsed = nil, nil
			if out.free != nil {
				out.free()
			}
			c.release(k.work)
			body.close()
			turn.file.Close()
			inspectStore(t, k.store)
			assertQueuedCleanup(t, k, baseline, fds, gen)
		})
	}
}

func TestQueuedHTTPStoredContinuation(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint(stream), func(t *testing.T) {
			fake := &fakeUpstream{}
			entered, send := make(chan struct{}), make(chan struct{})
			var calls atomic.Int32
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if calls.Add(1) == 1 {
					close(entered)
					<-send
				}
				fake.ServeHTTP(w, r)
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
			baseline, fds, gen := admissionSnapshot(k), keeperFDs(t, k), k.store.current
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(fmt.Sprintf(`{"input":"tiny","store":true,"stream":%t}`, stream)))
				res, err := srv.Client().Do(req)
				if err == nil {
					_, err = io.Copy(io.Discard, res.Body)
					res.Body.Close()
					if res.StatusCode != 200 {
						err = fmt.Errorf("status=%d", res.StatusCode)
					}
				}
				done <- err
			}()
			<-entered
			k.store.mu.Lock()
			close(send)
			waitQueuedResponse(t)
			held := admissionSnapshot(k).output
			k.store.mu.Unlock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if held != 0 {
				t.Errorf("actual handler queue output=%d", held)
			}
			history, ok := k.store.get("resp_interaction_1", [32]byte{})
			if !ok || len(history) != 3 {
				t.Fatalf("B not stored: %t %v", ok, history)
			}
			status, body, _ := post(t, srv, "", `{"previous_response_id":"resp_interaction_1","input":"next","store":true,"stream":false}`)
			if status != 200 {
				t.Fatalf("continuation %d %s", status, body)
			}
			received := fake.last()
			if len(received) != 4 {
				t.Fatalf("backend items=%d", len(received))
			}
			for i := range history {
				if !bytes.Equal(received[i], history[i]) {
					t.Errorf("backend history mismatch %d", i)
				}
			}
			assertQueuedCleanup(t, k, baseline, fds, gen)
		})
	}
}

// All terminal/read-ahead bytes arrive in one source read. The existing parser
// must resume after CR dispatch without losing its paired LF or the next event.
func TestQueuedSSEReadAheadFraming(t *testing.T) {
	for _, ending := range []string{"\n", "\r", "\r\n"} {
		t.Run(fmt.Sprintf("%q", ending), func(t *testing.T) {
			_, _, k := newTestKeeper(t)
			baseline, fds, gen := admissionSnapshot(k), keeperFDs(t, k), k.store.current
			input, err := k.spoolRequest(context.Background(), strings.NewReader(`{"input":"tiny"}`), nil)
			if err != nil {
				t.Fatal(err)
			}
			turn, apiErr := k.prepareDiskSnapshot(context.Background(), input, [32]byte{}, nil, false, false)
			input.Close()
			if apiErr != nil {
				t.Fatal(apiErr)
			}
			defer turn.file.Close()
			source := "\xef\xbb\xbf: before" + ending + "event: response.completed" + ending + `data: {"type":"response.completed",` + ending + `data: "response":{"id":"resp_frame","status":"completed","store":true,"output":["done"]}}` + ending + ending + ": after" + ending + "event: vendor.next" + ending + `data: {"type":"vendor.next","value":2}` + ending + ending + "data: [DONE]" + ending + ending
			var reference bytes.Buffer
			if err := readSSE(strings.NewReader(source), func(frame []byte) error { _, err := reference.Write(frame); return err }, func([]byte) error { return nil }); err != nil {
				t.Fatal(err)
			}
			var c collector
			defer c.release(k.work)
			var frames bytes.Buffer
			var kinds []string
			entered := make(chan struct{})
			done := make(chan error, 1)
			k.store.mu.Lock()
			go func() {
				accepted := false
				prepare := func() (*responseAcceptance, error) {
					if accepted || !c.done {
						return nil, nil
					}
					rid, output, _ := c.final()
					pending, err := k.prepareAcceptDisk(context.Background(), turn, rid, [32]byte{}, output, "", false)
					if err != nil {
						return nil, err
					}
					return &responseAcceptance{close: pending.close, commit: func() (bool, error) {
						close(entered)
						_, err := pending.commit(context.Background())
						accepted = err == nil
						return accepted, err
					}}, nil
				}
				err := readSSEOwned(strings.NewReader(source), func(frame []byte) error { _, err := frames.Write(frame); return err }, func(data *responseBuffer, suspend suspendResponseFrame) error {
					wire, free, parsed, err := k.responseFrameOwned(context.Background(), &c, data, true, true, prepare, suspend)
					if err != nil {
						return err
					}
					defer free()
					if parsed == nil {
						parsed, err = responseObject(wire)
						if err != nil {
							return err
						}
					}
					kinds = append(kinds, responseTypeOf(parsed))
					return nil
				}, k, k.work)
				done <- err
			}()
			<-entered
			waitQueuedResponse(t)
			held := admissionSnapshot(k).output
			k.store.mu.Unlock()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if held != 0 {
				t.Errorf("scanner/frame output=%d", held)
			}
			// Preserve the existing immediate-CR dispatch/EOF policy as well as
			// comments and every byte forwarded by the unsuspended parser.
			if frames.String() != reference.String() {
				t.Errorf("frame bytes changed after resume: got=%q want=%q", frames.String(), reference.String())
			}
			if strings.Join(kinds, ",") != "response.completed,vendor.next" {
				t.Errorf("event order=%v", kinds)
			}
			if _, _, ok := k.store.resolveID("resp_frame", [32]byte{}, false); !ok {
				t.Error("terminal not stored")
			}
			c.release(k.work)
			turn.file.Close()
			assertQueuedCleanup(t, k, baseline, fds, gen)
		})
	}
}

// Both socket paths must release raw output while keeping their actual pin and
// backend continuation contract, rather than succeeding via uncached serving.
func TestQueuedSocketStoredContinuation(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint(native), func(t *testing.T) {
			var srv *httptest.Server
			var k *keeper
			var last func() []json.RawMessage
			if native {
				fake, s, keeper := newWSNativeKeeper(t)
				srv, k, last = s, keeper, fake.last
			} else {
				fake, s, keeper := newKeeperFixture(t, true)
				srv, k, last = s, keeper, fake.last
			}
			baseline, fds, gen := admissionSnapshot(k), keeperFDs(t, k), k.store.current
			conn := dialWS(t, srv, "")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			// Warm route/socket setup before locking only the storage mutation queue.
			k.store.mu.Lock()
			if err := conn.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","input":"tiny"}`)); err != nil {
				k.store.mu.Unlock()
				t.Fatal(err)
			}
			waitQueuedResponse(t)
			held := admissionSnapshot(k).output
			k.store.mu.Unlock()
			var id string
			for id == "" {
				_, raw, err := conn.Read(ctx)
				if err != nil {
					t.Fatal(err)
				}
				obj, err := responseObject(raw)
				if err != nil {
					t.Fatal(err)
				}
				if responseTypeOf(obj) == "response.completed" {
					response, err := responseObject(obj["response"])
					if err != nil {
						t.Fatal(err)
					}
					id, _ = responseString(response["id"])
				}
			}
			if held != 0 {
				t.Errorf("socket queue output=%d", held)
			}
			history, ok := k.store.get(id, [32]byte{})
			if !ok || len(history) != 3 {
				t.Fatalf("socket history missing: id=%s items=%v", id, history)
			}
			result := wsCreate(t, conn, `{"type":"response.create","previous_response_id":"`+id+`","input":"next"}`)
			if responseID(result) == "" {
				t.Fatal("continuation not completed")
			}
			received := last()
			if len(received) != 4 {
				t.Fatalf("socket backend items=%d", len(received))
			}
			for i := range history {
				if !bytes.Equal(history[i], received[i]) {
					t.Errorf("socket backend history mismatch %d", i)
				}
			}
			conn.CloseNow()
			assertQueuedCleanup(t, k, baseline, fds, gen)
		})
	}
}

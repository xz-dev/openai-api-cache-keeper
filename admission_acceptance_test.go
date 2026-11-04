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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type admissionCredits struct{ input, output, disk int64 }

func admissionSnapshot(k *keeper) admissionCredits {
	k.work.mu.Lock()
	defer k.work.mu.Unlock()
	return admissionCredits{k.work.input, k.work.output, k.work.disk}
}
func waitAdmissionCredits(t *testing.T, k *keeper, want admissionCredits) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := admissionSnapshot(k); got == want && k.work.activeTurns.Load() == 0 && k.work.activeSockets.Load() == 0 && k.work.activeTasks.Load() == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("admission ownership leak: got=%+v want=%+v turns=%d sockets=%d tasks=%d", admissionSnapshot(k), want, k.work.activeTurns.Load(), k.work.activeSockets.Load(), k.work.activeTasks.Load())
}

func TestAdmission96ConcurrentBackendObservedHTTPTurns(t *testing.T) {
	const n = 96
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	gates := make([]chan struct{}, n)
	once := make([]sync.Once, n)
	for i := range gates {
		gates[i] = make(chan struct{})
	}
	release := func(i int) { once[i].Do(func() { close(gates[i]) }) }
	observed := make(chan int, n)
	var seen sync.Map
	fake := &fakeUpstream{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.Header.Get("X-Acceptance-ID"))
		if err != nil || id < 0 || id >= n {
			http.Error(w, "bad fixture id", 500)
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		if _, loaded := seen.LoadOrStore(id, true); loaded {
			http.Error(w, "duplicate fixture id", 500)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		observed <- id
		select {
		case <-gates[id]:
			fake.ServeHTTP(w, r)
		case <-ctx.Done():
			return
		case <-r.Context().Done():
			return
		}
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	baseline := admissionSnapshot(k)
	type result struct {
		id, status int
		body       string
		err        error
	}
	results := make(chan result, n)
	var workers sync.WaitGroup
	t.Cleanup(func() {
		for i := range gates {
			release(i)
		}
		cancel()
		workers.Wait()
	})
	for i := range n {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"input":"light","store":false,"stream":false}`))
			req.Header.Set("X-Acceptance-ID", strconv.Itoa(id))
			resp, err := srv.Client().Do(req)
			out := result{id: id, err: err}
			if err == nil {
				out.status = resp.StatusCode
				b, e := io.ReadAll(resp.Body)
				resp.Body.Close()
				out.body, out.err = string(b), e
			}
			results <- out
		}(i)
	}
	for range n {
		select {
		case <-observed:
		case out := <-results:
			t.Fatalf("turn finished before all96 reached backend: id=%d status=%d err=%v body=%s", out.id, out.status, out.err, out.body)
		case <-ctx.Done():
			t.Fatal("96 distinct admitted upstream turns did not reach barrier")
		}
	}
	select {
	case out := <-results:
		t.Fatalf("barrier released prematurely: %+v", out)
	default:
	}
	// Admission, not simultaneous scratch exhaustion: all96 are live first;
	// complete in eight lanes below the unchanged16-MiB output scratch pool.
	for i := 0; i < 8; i++ {
		release(i)
	}
	next := 8
	for range n {
		select {
		case out := <-results:
			if out.err != nil || out.status != 200 || !strings.Contains(out.body, `"status":"completed"`) {
				t.Fatalf("admitted turn incomplete: %+v", out)
			}
			if next < n {
				release(next)
				next++
			}
		case <-ctx.Done():
			t.Fatal("admitted turns did not complete")
		}
	}
	workers.Wait()
	waitAdmissionCredits(t, k, baseline)
	t.Log("observed96 distinct upstream requests before any completion;96 completedHTTP200; owned credits returned to baseline")
}

func TestAdmission65RetainedCompletedWebSockets(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(fmt.Sprint("native=", native), func(t *testing.T) {
			var srv *httptest.Server
			var k *keeper
			if native {
				_, srv, k = newWSNativeKeeper(t)
			} else {
				_, srv, k = newLocalTestKeeper(t)
			}
			baseline := admissionSnapshot(k)
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
			connections := make([]*websocket.Conn, 0, 65)
			events := make([]chan map[string]any, 0, 65)
			var readers sync.WaitGroup
			cleanup := func() {
				for _, c := range connections {
					c.CloseNow()
				}
				cancel()
				readers.Wait()
			}
			t.Cleanup(cleanup)
			waitTerminal := func(ch <-chan map[string]any) map[string]any {
				for {
					select {
					case ev := <-ch:
						if ev == nil {
							t.Fatal("reader ended before terminal")
						}
						switch ev["type"] {
						case "response.completed":
							return ev
						case "error", "response.failed":
							t.Fatalf("light WS turn failed: %v", ev)
						}
					case <-ctx.Done():
						t.Fatal("WS turn did not complete")
					}
				}
			}
			var firstID string
			for i := 0; i < 65; i++ {
				c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(srv.URL, "http")+"/v1/responses", nil)
				if err != nil {
					t.Fatalf("retained completed session%d rejected: %v", i+1, err)
				}
				connections = append(connections, c)
				ch := make(chan map[string]any, 16)
				events = append(events, ch)
				readers.Add(1)
				go func() {
					defer readers.Done()
					defer close(ch)
					for {
						_, body, err := c.Read(ctx)
						if err != nil {
							return
						}
						var ev map[string]any
						if json.Unmarshal(body, &ev) != nil {
							return
						}
						select {
						case ch <- ev:
						case <-ctx.Done():
							return
						}
					}
				}()
				if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"light","store":false}`)); err != nil {
					t.Fatal(err)
				}
				ev := waitTerminal(ch)
				if i == 0 {
					firstID = responseID(ev)
				}
			}
			for i, c := range connections {
				if err := c.Ping(ctx); err != nil {
					t.Fatalf("retained session%d Ping failed after65th completion: %v", i+1, err)
				}
			}
			if err := connections[0].Write(ctx, websocket.MessageText, []byte(fmt.Sprintf(`{"type":"response.create","model":"m","input":"continue","store":false,"previous_response_id":%q}`, firstID))); err != nil {
				t.Fatal(err)
			}
			waitTerminal(events[0])
			cleanup()
			waitAdmissionCredits(t, k, baseline)
			t.Log("65 completed live WS sessions retained; all65Ping; first session continuation completed; readers joined and owned credits returned")
		})
	}
}

func TestAdmission65RetainedCompletedBackgroundTasks(t *testing.T) {
	var creates atomic.Int32
	u := gateUpstream(t, &creates, 65, func(w io.Writer, flush func()) {
		io.WriteString(w, gateEvent("response.created", ""))
		flush()
		io.WriteString(w, gateEvent("response.completed", ""))
		flush()
	})
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	baseline := admissionSnapshot(k)
	tasks := make([]*responseTask, 0, 65)
	t.Cleanup(func() {
		for _, task := range tasks {
			task.expire()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	for i := 0; i < 65; i++ {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"input":"light","store":false,"background":true,"stream":true}`))
		req.Header.Set("Authorization", "Bearer owner")
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil || resp.StatusCode != 200 || !strings.Contains(string(body), "response.completed") {
			t.Fatalf("background task%d did not complete: status=%d err=%v body=%s", i+1, resp.StatusCode, err, body)
		}
		var id string
		for _, line := range strings.Split(string(body), "\n") {
			if strings.HasPrefix(line, "data: ") {
				var ev map[string]any
				if json.Unmarshal([]byte(strings.TrimSpace(strings.TrimPrefix(line, "data: "))), &ev) == nil && ev["type"] == "response.completed" {
					id = responseID(ev)
				}
			}
		}
		k.cacheMu.Lock()
		task := k.tasks[id]
		k.cacheMu.Unlock()
		if task == nil {
			t.Fatal("completed task was not retained:", id)
		}
		tasks = append(tasks, task)
	}
	if creates.Load() != 65 || k.work.activeTasks.Load() != 65 {
		t.Fatalf("task admission hidden cap: upstream=%d retained=%d", creates.Load(), k.work.activeTasks.Load())
	}
	for _, task := range tasks {
		select {
		case <-task.finished:
		case <-ctx.Done():
			t.Fatal("task producer did not finish")
		}
		task.expire()
	}
	waitAdmissionCredits(t, k, baseline)
	t.Log("65 completed background tasks retained simultaneously;65 upstream generations; expiry joined producers and returned owned credits")
}

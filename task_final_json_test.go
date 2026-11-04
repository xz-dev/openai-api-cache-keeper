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
	"strings"
	"sync"
	"testing"
	"time"
)

type finalJSONDeadlineWriter struct {
	header       http.Header
	entered      chan int
	abort        chan struct{}
	callback     chan struct{}
	join         chan struct{}
	writeOnce    sync.Once
	deadlineOnce sync.Once
}

func (w *finalJSONDeadlineWriter) Header() http.Header { return w.header }
func (w *finalJSONDeadlineWriter) WriteHeader(int)     {}
func (w *finalJSONDeadlineWriter) Write(p []byte) (int, error) {
	w.writeOnce.Do(func() { w.entered <- len(p) })
	<-w.abort
	return 0, context.Canceled
}
func (w *finalJSONDeadlineWriter) SetWriteDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		w.deadlineOnce.Do(func() {
			close(w.abort)
			close(w.callback)
			<-w.join
		})
	}
	return nil
}

func TestFinalJSONOwnsBytesUntilCancellationCallbackJoins(t *testing.T) {
	for _, expire := range []bool{false, true} {
		t.Run(fmt.Sprint(expire), func(t *testing.T) {
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = io.WriteString(w, `{}`) }))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
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
			defer stop()
			finished := make(chan struct{})
			close(finished)
			task := &responseTask{keeper: k, state: state, id: keeperResponsePrefix + "final", cancel: stop, log: f, changed: make(chan struct{}), ready: make(chan struct{}), finished: finished, stream: true, done: true}
			raw := []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_up","object":"response","status":"completed","output":[{"text":%q}]}}`, strings.Repeat("x", 128<<10)))
			if err := task.record(context.Background(), raw, nil); err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, "/v1/responses/"+task.id, nil)
			task.owner = k.ownerOf(req)
			k.cacheMu.Lock()
			k.tasks[task.id] = task
			k.cacheMu.Unlock()
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			w := &finalJSONDeadlineWriter{header: make(http.Header), entered: make(chan int, 1), abort: make(chan struct{}), callback: make(chan struct{}), join: make(chan struct{})}
			var joinOnce sync.Once
			releaseCallback := func() { joinOnce.Do(func() { close(w.join) }) }
			defer releaseCallback()
			done := make(chan struct{})
			go func() { defer close(done); k.taskOperation(w, req.WithContext(ctx)) }()
			var n int
			select {
			case n = <-w.entered:
			case <-time.After(3 * time.Second):
				t.Fatal("final response did not reach writer")
			}
			if got := admissionSnapshot(k).output; got < int64(n) {
				t.Fatalf("blocked final bytes=%d credit=%d", n, got)
			}
			if !task.mu.TryLock() {
				t.Fatal("blocked network output holds task mutex")
			}
			task.mu.Unlock()
			if expire {
				expired := make(chan struct{})
				go func() { task.expire(); close(expired) }()
				select {
				case <-expired:
				case <-time.After(3 * time.Second):
					t.Fatal("task expiry blocked behind client output")
				}
			} else {
				cancel()
			}
			select {
			case <-w.callback:
			case <-time.After(3 * time.Second):
				t.Fatal("cancellation did not interrupt selected write")
			}
			select {
			case <-done:
				t.Fatal("output owner ended before callback joined")
			default:
			}
			if got := admissionSnapshot(k).output; got < int64(n) {
				t.Fatalf("output released before callback join: %d < %d", got, n)
			}
			f.mu.Lock()
			open := f.File != nil
			f.mu.Unlock()
			if !open {
				t.Fatal("selected spool closed before write joined")
			}
			releaseCallback()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Fatal("selected write failed to finish after cancellation join")
			}
			task.expire()
			waitAdmissionCredits(t, k, baseline)
		})
	}
}

func TestTaskStatusQuotesBoundedUTF8Chunks(t *testing.T) {
	for _, id := range []string{"", "resp_simple", strings.Repeat("a", 1023) + "猫\"\\\n\x00<&>\u2028\u2029" + strings.Repeat("z", 256<<10)} {
		var output bytes.Buffer
		if err := writeTaskStatus(&output, id, "cancelled"); err != nil {
			t.Fatal(err)
		}
		var got struct {
			ID         string   `json:"id"`
			Object     string   `json:"object"`
			Background bool     `json:"background"`
			Status     string   `json:"status"`
			Output     []string `json:"output"`
		}
		if err := json.Unmarshal(output.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.ID != id || got.Object != "response" || !got.Background || got.Status != "cancelled" || len(got.Output) != 0 {
			t.Fatal("chunked status changed identity or response fields")
		}
	}
}

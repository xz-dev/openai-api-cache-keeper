package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestCancelledBlockedHTTPAndReplaySelectedStream(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		for _, replay := range []bool{false, true} {
			t.Run(fmt.Sprintf("http2=%v/replay=%v", h2, replay), func(t *testing.T) {
				var sent atomic.Int64
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, _ = io.Copy(io.Discard, r.Body)
					w.WriteHeader(http.StatusBadGateway)
					chunk := strings.Repeat("x", 32<<10)
					for i := 0; i < 1024; i++ {
						if _, err := io.WriteString(w, chunk); err != nil {
							return
						}
						sent.Add(int64(len(chunk)))
					}
					<-r.Context().Done()
				}))
				t.Cleanup(up.Close)
				u, _ := url.Parse(up.URL)
				_, _, k := newTestKeeper(t, func(cfg *config) { cfg.upstream = u; cfg.maxStoreBytes = 32 << 20 })
				var task *responseTask
				if replay {
					log, err := k.newSpool()
					if err != nil {
						t.Fatal(err)
					}
					_, cancel := context.WithCancel(context.Background())
					task = &responseTask{keeper: k, log: log, cancel: cancel, changed: make(chan struct{}), done: true}
					if err := task.append([]byte(strings.Repeat("r", 8<<20))); err != nil {
						t.Fatal(err)
					}
					t.Cleanup(task.expire)
				}
				cancelRequest := make(chan context.CancelFunc, 1)
				handlerDone := make(chan struct{})
				remote := make(chan string, 1)
				server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/fast" {
						_, _ = io.WriteString(w, r.RemoteAddr)
						return
					}
					defer close(handlerDone)
					remote <- r.RemoteAddr
					if replay {
						cancelRequest <- task.expire
						_ = task.follow(w, r, -1, false)
						return
					}
					ctx, cancel := context.WithCancel(r.Context())
					defer cancel()
					cancelRequest <- cancel
					k.serveHTTP(w, r.WithContext(ctx))
				}))
				server.EnableHTTP2 = h2
				if h2 {
					server.StartTLS()
				} else {
					server.Start()
				}
				t.Cleanup(server.Close)
				client := server.Client()
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				defer cancel()
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"input":"small","store":false,"stream":false}`))
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { resp.Body.Close() })
				if (resp.ProtoMajor == 2) != h2 {
					t.Fatalf("fixture negotiated %s, http2=%v", resp.Proto, h2)
				}
				originalRemote := <-remote
				stop := <-cancelRequest
				t.Cleanup(stop)
				// The client never reads this body. Eight MiB exceeds HTTP/2's
				// stream window and the local HTTP/1 receive socket capacity.
				if !replay {
					before, stableSince := sent.Load(), time.Now()
					for {
						if current := sent.Load(); current != before {
							before, stableSince = current, time.Now()
						}
						if before > 0 && before < 32<<20 && time.Since(stableSince) >= 150*time.Millisecond {
							break
						}
						select {
						case <-ctx.Done():
							t.Fatalf("no stable real transport backpressure before existing fixture deadline: %d", before)
						case <-time.After(10 * time.Millisecond):
						}
					}
					t.Logf("observed stable upstream write plateau at %d bytes for at least 150ms", before)
				} else {
					time.Sleep(150 * time.Millisecond)
				}
				select {
				case <-handlerDone:
					t.Fatal("handler ended before cancellation; write was not blocked")
				default:
				}
				started := time.Now()
				stop()
				select {
				case <-handlerDone:
				case <-time.After(2 * time.Second):
					stack := make([]byte, 1<<20)
					n := runtime.Stack(stack, true)
					for _, entry := range strings.Split(string(stack[:n]), "\n\n") {
						if strings.Contains(entry, "serveHTTP") || strings.Contains(entry, "interruptCancelledWrites") {
							t.Log(entry)
						}
					}
					t.Fatal("real cancellation did not interrupt blocked selected-stream output")
				}
				resp.Body.Close()
				fast, err := client.Get(server.URL + "/fast")
				if err != nil {
					t.Fatal(err)
				}
				got, err := io.ReadAll(fast.Body)
				fast.Body.Close()
				if err != nil || fast.StatusCode != 200 || (h2 && string(got) != originalRemote) {
					t.Fatalf("cancellation damaged unrelated/reused connection: status=%d err=%v", fast.StatusCode, err)
				}
				k.work.mu.Lock()
				input, output, disk := k.work.input, k.work.output, k.work.disk
				k.work.mu.Unlock()
				if input != 0 || output != 0 || disk != 0 {
					t.Fatalf("cancelled output leaked input/output/disk credits: %d/%d/%d", input, output, disk)
				}
				t.Logf("blocked selected stream cancelled in %s; HTTP/2 peer connection preserved=%v", time.Since(started), h2)
			})
		}
	}
}

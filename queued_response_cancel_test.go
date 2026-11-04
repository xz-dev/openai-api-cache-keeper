package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestQueuedHTTPResponseCancellationCleanup(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, staged := range []bool{false, true} {
			t.Run(fmt.Sprintf("stream=%t/staged=%t", stream, staged), func(t *testing.T) {
				fake := &fakeUpstream{}
				entered, send := make(chan struct{}), make(chan struct{})
				up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-send; fake.ServeHTTP(w, r) }))
				defer up.Close()
				u, _ := url.Parse(up.URL)
				_, _, k := newTestKeeper(t, func(c *config) { c.upstream = u })
				baseline, fds, gen := admissionSnapshot(k), keeperFDs(t, k), k.store.current
				handlerDone := make(chan struct{})
				stop := make(chan context.CancelFunc, 1)
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(handlerDone)
					ctx, cancel := context.WithCancel(r.Context())
					defer cancel()
					stop <- cancel
					k.ServeHTTP(w, r.WithContext(ctx))
				}))
				defer srv.Close()
				stage, releaseStage := make(chan struct{}), make(chan struct{})
				if staged {
					k.store.stageCommitted = func() error { close(stage); <-releaseStage; return nil }
				}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				done := make(chan error, 1)
				go func() {
					req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(fmt.Sprintf(`{"input":"tiny","store":true,"stream":%t}`, stream)))
					res, err := srv.Client().Do(req)
					if err == nil {
						_, err = io.Copy(io.Discard, res.Body)
						res.Body.Close()
					}
					done <- err
				}()
				<-entered
				if !staged {
					k.store.mu.Lock()
				}
				close(send)
				if staged {
					<-stage
				} else {
					waitQueuedResponse(t)
				}
				held := admissionSnapshot(k).output
				if held != 0 {
					t.Errorf("cancel boundary output=%d", held)
				}
				(<-stop)()
				if staged {
					close(releaseStage)
				} else {
					k.store.mu.Unlock()
				}
				select {
				case <-handlerDone:
				case <-ctx.Done():
					t.Fatal("cancel did not join response producer")
				}
				<-done // Client may see EOF or a closed stream; storage/ownership are authoritative.
				if _, _, ok := k.store.resolveID("resp_interaction_1", [32]byte{}, false); ok {
					t.Error("cancelled response alias committed")
				}
				inspectStore(t, k.store)
				assertQueuedCleanup(t, k, baseline, fds, gen)
			})
		}
	}
}

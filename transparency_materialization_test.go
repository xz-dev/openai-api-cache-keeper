package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Freeze one actual event-decode/materialization action, not an upstream
// request: generation waiting alone does not hold the keeper's phase token.
func TestTransparencyUnrelatedTurnDuringPausedMaterialization(t *testing.T) {
	_, srv, _ := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	var first atomic.Bool
	var a sync.WaitGroup
	request := func(ctx context.Context, input string) (int, string, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(input))
		if err != nil {
			return 0, "", err
		}
		req.Header.Set("Authorization", "Bearer independent-materialization-fixture")
		resp, err := srv.Client().Do(req)
		if err != nil {
			return 0, "", err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(body), err
	}
	status, body, err := request(ctx, `{"input":"control","store":false,"stream":false}`)
	if err != nil || status != http.StatusOK || !strings.Contains(body, `"status":"completed"`) {
		cancel()
		t.Fatalf("quiet control failed: status=%d err=%v body=%s", status, err, body)
	}
	collectEventCalls = func() {
		if first.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
			}
		}
	}
	// Cleanup is registered before any assertion/goroutine using the gate.
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(release) })
		cancel()
		a.Wait()
		collectEventCalls = nil
	})
	aResult := make(chan error, 1)
	a.Add(1)
	go func() {
		defer a.Done()
		status, body, err := request(ctx, `{"input":"slow-event","store":false,"stream":true}`)
		if err == nil && (status != http.StatusOK || !strings.Contains(body, "response.completed")) {
			err = fmt.Errorf("slow session did not complete: status=%d body=%s", status, body)
		}
		aResult <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first session never entered actual materialization")
	}
	t.Log("one slow event decoder parked; unrelated turn must complete before release")
	bctx, bcancel := context.WithTimeout(ctx, 10*time.Second)
	defer bcancel()
	started := time.Now()
	status, body, err = request(bctx, `{"input":"independent-fast","store":false,"stream":false}`)
	elapsed := time.Since(started)
	select {
	case <-release:
		t.Fatal("invalid fixture: materialization gate released before unrelated turn completed")
	default:
	}
	if err != nil || status != http.StatusOK || !strings.Contains(body, `"status":"completed"`) {
		t.Fatalf("unrelated public HTTP turn blocked/rejected by another session's paused materialization: elapsed=%s status=%d err=%v body=%s", elapsed, status, err, body)
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-aResult; err != nil {
		t.Fatal(err)
	}
	t.Logf("unrelated turn completed in %s while slow materialization stayed paused", elapsed)
}

package main

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// A source remains inside actual planChain IO, outside any bbolt commit.
// Unrelated public store:true completion must not need this source's release.
func TestIndependentSlowStoreSourceBlocksUnrelatedStoredHTTP(t *testing.T) {
	_, srv, k := newTestKeeper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	releaseA := func() { once.Do(func() { close(release) }) }
	aDone := make(chan error, 1)
	defer func() {
		releaseA()
		if err := <-aDone; err != nil {
			t.Error(err)
		}
	}()
	go func() {
		source := func() itemSource {
			next := sliceItems(items(`"slow-source"`))()
			first := true
			return func(dst []byte) ([]byte, error) {
				if first {
					first = false
					select {
					case <-entered:
					default:
						close(entered)
					}
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				return next(dst)
			}
		}
		_, _, err := k.store.acceptWithBudget(ctx, "slow-source", [32]byte{}, source, "", true, false, func() (int64, func()) { return k.reserveHistoryCache(32) })
		aDone <- err
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("source did not enter")
	}
	request := func(store bool) error {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(fmt.Sprintf(`{"input":"small","store":%t,"stream":false}`, store)))
		req.Header.Set("Authorization", "Bearer storage-independence")
		res, err := srv.Client().Do(req)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		if err != nil {
			return err
		}
		if res.StatusCode != 200 || !strings.Contains(string(b), `"status":"completed"`) {
			return fmt.Errorf("status=%d body=%s", res.StatusCode, b)
		}
		return nil
	}
	if err := request(false); err != nil {
		t.Fatal("fitting store:false control", err)
	}
	bDone := make(chan error, 1)
	go func() { bDone <- request(true) }()
	blocked := false
	select {
	case err := <-bDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(200 * time.Millisecond):
		blocked = true
	}
	if blocked {
		buf := make([]byte, 1<<20)
		n := runtime.Stack(buf, true)
		for _, block := range strings.Split(string(buf[:n]), "\n\n") {
			if strings.Contains(block, "(*store).acceptWithBudget") || strings.Contains(block, "(*store).planChain") {
				t.Log(block)
			}
		}
		k.work.mu.Lock()
		t.Logf("while source parked: input=%d output=%d disk=%d", k.work.input, k.work.output, k.work.disk)
		k.work.mu.Unlock()
	}
	releaseA()
	if blocked {
		select {
		case err := <-bDone:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("stored B did not resume")
		}
		t.Error("unrelated fitting public store:true response waited for slow source outside atomic commit")
	}
}

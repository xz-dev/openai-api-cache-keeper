package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// rssMiB reports current RSS via runtime/metrics-free /proc.
func rssBytes() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.Sys)
}

// TestWSIdleSocketRSS is a measurement, not an assertion: it reports RSS with
// 0 and 64 idle sockets and with 16 concurrent turns.
func TestWSIdleSocketRSS(t *testing.T) {
	up := &benchUpstream{}
	upSrv := httptest.NewServer(up)
	defer upSrv.Close()
	u, _ := url.Parse(upSrv.URL)
	cfg := config{upstream: u, ttl: time.Minute, maxStoreBytes: 64 << 20, maxRequestBytes: 16 << 20,
		storeDir: t.TempDir(), storeMetaBytes: 16 << 20, storePinBytes: 8 << 20, storeCompactBytes: 128 << 20}
	k := newKeeper(cfg)
	srv := httptest.NewServer(k)
	defer srv.Close()
	defer k.store.close()

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	idle := rssBytes()

	var conns []*websocket.Conn
	wsURL := strings.Replace(srv.URL, "http", "ws", 1) + "/v1/responses"
	for i := 0; i < 64; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		c, _, err := websocket.Dial(ctx, wsURL, nil)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	defer func() {
		for _, c := range conns {
			c.CloseNow()
		}
	}()
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	s64 := rssBytes()
	perIdle := float64(s64-idle) / 64 / 1024
	t.Logf("RSS idle=%.1fMB with-64-idle-sockets=%.1fMB (%.1fKiB/socket)",
		float64(idle)/(1<<20), float64(s64)/(1<<20), perIdle)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				ctx := context.Background()
				if err := conns[i].Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"x"}`)); err != nil {
					return
				}
				for {
					_, raw, err := conns[i].Read(ctx)
					if err != nil {
						return
					}
					var ev struct{ Type string }
					_ = json.Unmarshal(raw, &ev)
					if ev.Type == "response.completed" || ev.Type == "error" {
						break
					}
				}
			}
		}(i)
	}
	wg.Wait()
	time.Sleep(50 * time.Millisecond)
	s16 := rssBytes()
	t.Logf("RSS after 16 concurrent sockets x5 turns=%.1fMB entries=%s", float64(s16)/(1<<20), fmt.Sprint(benchEntries(up)))
}

func benchEntries(u *benchUpstream) int { return u.n }

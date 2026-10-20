package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestFinalManySSELines(t *testing.T) {
	body := strings.Repeat(":\n", 200000) + "\n"
	m := newWorkingSet()
	var c collector
	runtime.GC()
	var a, b runtime.MemStats
	runtime.ReadMemStats(&a)
	lines := 0
	err := readSSE(strings.NewReader(body), func([]byte) error { lines++; return nil }, func(raw []byte) error { return c.boundedEvent(raw, m) })
	runtime.ReadMemStats(&b)
	t.Logf("source=%d lines=%d charged=%d totalAllocDelta=%d heapAllocDelta=%d err=%v", len(body), lines, m.output, b.TotalAlloc-a.TotalAlloc, int64(b.HeapAlloc)-int64(a.HeapAlloc), err)
	if err == nil && lines > maxHistoryItems {
		t.Error("unbounded line metadata accepted outside global output budget")
	}
}

// Actual malformed Content-Length response, no custom RoundTripper.
func TestFinalTruncatedWireSSE(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		raw := `data: {"type":"response.completed","response":{"id":"resp_wire","status":"completed","output":[]}}`
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Content-Length", fmt.Sprint(len(raw)+10))
		io.WriteString(w, raw)
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) { c.upstream = u })
	status, body, _ := post(t, srv, "Bearer wire", `{"input":"A","stream":true}`)
	n, _ := k.store.stats()
	t.Logf("status=%d stored=%d body=%s", status, n, body)
	if n != 0 || strings.Contains(body, "response.completed") {
		t.Error("real truncated wire response committed before framing error")
	}
}

func TestFinalEarlyResponseRealHTTP(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	got := make(chan string, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			got <- err.Error()
			return
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		// Read through headers only. Reply before consuming the large body.
		var hdr bytes.Buffer
		one := make([]byte, 1)
		for !strings.HasSuffix(hdr.String(), "\r\n\r\n") {
			if _, err := conn.Read(one); err != nil {
				got <- err.Error()
				return
			}
			hdr.Write(one)
		}
		resp := `{"object":"response","id":"resp_early_real","status":"completed","output":[]}`
		fmt.Fprintf(conn, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(resp), resp)
		time.Sleep(50 * time.Millisecond)
		n, err := io.Copy(io.Discard, conn)
		got <- fmt.Sprintf("wireRequestBytes=%d err=%v", n, err)
	}()
	u, _ := url.Parse("http://" + ln.Addr().String())
	_, srv, _ := newTestKeeper(t, func(c *config) {
		c.upstream = u
		c.maxRequestBytes = 32 << 20
		c.maxStoreBytes = 64 << 20
		c.storeCompactBytes = 128 << 20
	})
	status, body, _ := post(t, srv, "Bearer early", `{"input":"`+strings.Repeat("x", 8<<20)+`"}`)
	t.Logf("status=%d clientBody=%s upstream=%s", status, body, <-got)
}

var _ = context.Background

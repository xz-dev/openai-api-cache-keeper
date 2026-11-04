package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestTransparencyWithinLimitTwoMiBResponse(t *testing.T) {
	body := `{"id":"response-two-mib","object":"response","status":"completed","store":false,"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + strings.Repeat("x", 2<<20) + `"}]}]}`
	if len(body) >= maxResponseBytes {
		t.Fatal("fixture exceeded the existing per-response ceiling")
	}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(up.Close)
	u, err := url.Parse(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, srv, _ := newTestKeeper(t, func(cfg *config) { cfg.upstream = u })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"input":"small","store":false,"stream":false}`))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || string(got) != body {
		t.Fatalf("within-limit %d-byte response failed: status=%d returned_bytes=%d prefix=%s", len(body), resp.StatusCode, len(got), got[:min(256, len(got))])
	}
}

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

// largeRelay posts request to a keeper whose upstream answers with body and
// returns the relayed status and bytes, then checks output credits return.
func largeRelay(t *testing.T, contentType, body, request string) (int, string) {
	t.Helper()
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", contentType)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(up.Close)
	u, err := url.Parse(up.URL)
	if err != nil {
		t.Fatal(err)
	}
	// Store budget large enough to retain the turn, so store:true exercises
	// the intended id/store rewrite; output/response limits are unchanged.
	_, srv, k := newTestKeeper(t, func(cfg *config) { cfg.upstream = u; cfg.maxStoreBytes = 64 << 20 })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(request))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for k.work.activeTurns.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if out := admissionSnapshot(k).output; out != 0 {
		t.Errorf("output credits not returned: %d", out)
	}
	return resp.StatusCode, string(got)
}

func TestTransparencyWithinLimitTwoMiBStoredResponse(t *testing.T) {
	body := `{"id":"response-two-mib","object":"response","status":"completed","store":false,"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"` + strings.Repeat("x", 2<<20) + `"}]}]}`
	want := strings.Replace(strings.Replace(body, `"id":"response-two-mib"`, `"id":"resp_response-two-mib"`, 1), `"store":false`, `"store":true`, 1)
	status, got := largeRelay(t, "application/json", body, `{"input":"small","store":true,"stream":false}`)
	if status != http.StatusOK || got != want {
		t.Fatalf("stored %d-byte response: status=%d returned_bytes=%d prefix=%s", len(body), status, len(got), got[:min(256, len(got))])
	}
}

// Events are capped at maxEventBytes (1MiB), so a legal ~2MiB streamed
// response carries its output in several item events below that cap.
func TestTransparencyWithinLimitTwoMiBStreamResponse(t *testing.T) {
	for _, stored := range []bool{false, true} {
		t.Run(fmt.Sprint(stored), func(t *testing.T) {
			created := `{"type":"response.created","sequence_number":0,"response":{"id":"up_sse","object":"response","status":"in_progress","store":false,"output":[]}}`
			var b, want strings.Builder
			emit := func(payload, published string) {
				b.WriteString("data: " + payload + "\n\n")
				want.WriteString("data: " + published + "\n\n")
			}
			rewrite := func(s string) string {
				if !stored {
					return s
				}
				return strings.Replace(strings.Replace(s, `"id":"up_sse"`, `"id":"resp_up_sse"`, 1), `"store":false`, `"store":true`, 1)
			}
			emit(created, rewrite(created))
			for i := range 3 {
				item := fmt.Sprintf(`{"type":"response.output_item.done","sequence_number":%d,"output_index":%d,"item":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"%s"}]}}`, i+1, i, strings.Repeat("y", 700<<10))
				emit(item, item)
			}
			completed := `{"type":"response.completed","sequence_number":4,"response":{"id":"up_sse","object":"response","status":"completed","store":false,"output":[]}}`
			emit(completed, rewrite(completed))
			status, got := largeRelay(t, "text/event-stream", b.String(), fmt.Sprintf(`{"input":"small","store":%t,"stream":true}`, stored))
			if status != http.StatusOK || got != want.String() {
				t.Fatalf("stream %d bytes: status=%d returned_bytes=%d tail=%s", b.Len(), status, len(got), got[max(0, len(got)-256):])
			}
		})
	}
}

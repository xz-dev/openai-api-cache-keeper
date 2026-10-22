package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// Reachability of the missing aggregate accepted-history byte guard through
// ordinary local HTTP servers and the production transport.
func TestReviewCoreOversizedTerminalRealHTTP(t *testing.T) {
	for _, stream := range []bool{false, true} {
		t.Run(fmt.Sprint("sse=", stream), func(t *testing.T) {
			output := `"` + strings.Repeat("b", 512<<10) + `"`
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Error(err)
					return
				}
				if stream {
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_review_oversized\",\"status\":\"completed\",\"output\":[%s]}}\n\n", output)
				} else {
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintf(w, `{"id":"resp_review_oversized","status":"completed","output":[%s]}`, output)
				}
			}))
			defer upstream.Close()
			u, _ := url.Parse(upstream.URL)
			_, srv, k := newTestKeeper(t, func(c *config) {
				c.upstream = u
				c.maxStoreBytes = 256 << 20
				c.storeMetaBytes = 8 << 20
				c.storePinBytes = 128 << 20
				c.storeCompactBytes = 512 << 20
			})
			req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", nil)
			req.Header.Set("Authorization", "Bearer review")
			owner := k.ownerOf(req)
			one := json.RawMessage(`"` + strings.Repeat("a", (256<<10)-1026) + `"`)
			full := make([]json.RawMessage, 256)
			for i := range full {
				full[i] = one
			}
			if _, err := k.store.put("seed", owner, "", full); err != nil {
				t.Fatal(err)
			}
			status, body, _ := post(t, srv, "Bearer review", fmt.Sprintf(`{"model":"m","input":[],"previous_response_id":"seed","stream":%v}`, stream))
			_, _, stored := k.store.resolveID("resp_review_oversized", owner, false)
			status2, body2, _ := post(t, srv, "Bearer review", `{"model":"m","input":[],"previous_response_id":"resp_review_oversized"}`)
			t.Logf("terminal status=%d terminal_success=%v stored=%v continuation=%d body=%s", status, strings.Contains(body, `"status":"completed"`), stored, status2, body2)
			// Previous seed itself remains intact; no probe sends real inference.
			raw, err := k.spoolRequest(context.Background(), strings.NewReader(`{"model":"m","input":[],"previous_response_id":"seed"}`))
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			turn, ae := k.prepareDisk(context.Background(), raw, owner, func(id string) ([32]byte, bool) { tip, _, ok := k.store.resolveID(id, owner, false); return tip, ok }, false)
			if ae != nil {
				t.Fatal(ae)
			}
			turn.file.Close()
			if stored && status2 == 413 {
				t.Fatal("terminal success publishes an immediately unreplayable history on real wire")
			}
		})
	}
}

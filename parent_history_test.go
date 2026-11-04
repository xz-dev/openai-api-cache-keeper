package main

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"github.com/coder/websocket"
)

// A keeper-owned parent ID larger than any metadata window must still expand
// its stored history; dropping it would silently send only the new turn.
func TestLargeParentIDKeepsHistory(t *testing.T) {
	for _, local := range []bool{true, false} {
		t.Run(fmt.Sprint(local), func(t *testing.T) {
			large := "resp_" + strings.Repeat("p", 256<<10)
			var mu sync.Mutex
			var bodies []string
			event := func() string {
				mu.Lock()
				first := len(bodies) == 1
				mu.Unlock()
				id := "resp_small"
				if first {
					id = large
				}
				return fmt.Sprintf(`{"type":"response.completed","response":{"id":%q,"object":"response","status":"completed","store":false,"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"alpha-reply"}]}]}}`, id)
			}
			record := func(b []byte) {
				mu.Lock()
				bodies = append(bodies, string(b))
				mu.Unlock()
			}
			up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if !isUpgradeRequest(r) {
					b, _ := io.ReadAll(r.Body)
					record(b)
					w.Header().Set("Content-Type", "text/event-stream")
					fmt.Fprintf(w, "data: %s\n\n", event())
					return
				}
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				conn.SetReadLimit(4 << 20)
				for {
					_, b, err := conn.Read(r.Context())
					if err != nil {
						return
					}
					record(b)
					if err := conn.Write(r.Context(), websocket.MessageText, []byte(event())); err != nil {
						return
					}
				}
			}))
			defer up.Close()
			u, _ := url.Parse(up.URL)
			_, srv, k := newKeeperFixture(t, local, func(c *config) { c.upstream = u })
			baseline := admissionSnapshot(k)
			conn := dialWS(t, srv, "Bearer a")
			conn.SetReadLimit(1 << 20)
			if ev := wsCreate(t, conn, `{"type":"response.create","model":"m","input":"alpha-turn","store":false}`); responseID(ev) != large {
				t.Fatalf("first turn id not retained: %v", ev["type"])
			}
			ev := wsCreate(t, conn, `{"type":"response.create","model":"m","input":"beta-turn","store":false,"previous_response_id":"`+large+`"}`)
			if responseID(ev) != "resp_small" {
				t.Fatalf("continuation failed: %v", ev)
			}
			mu.Lock()
			got := append([]string(nil), bodies...)
			mu.Unlock()
			if len(got) != 2 {
				t.Fatalf("upstream turns=%d", len(got))
			}
			for _, want := range []string{"alpha-turn", "alpha-reply", "beta-turn"} {
				if !strings.Contains(got[1], want) {
					t.Errorf("continuation upstream body lacks %q: parent history dropped", want)
				}
			}
			if strings.Contains(got[1], large) {
				t.Error("keeper-owned parent ID leaked upstream instead of expanding history")
			}
			conn.CloseNow()
			waitAdmissionCredits(t, k, baseline)
		})
	}
}

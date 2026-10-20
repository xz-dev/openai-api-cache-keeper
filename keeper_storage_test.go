package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestWSPrivateTTLTransitionAndFailedReplacement(t *testing.T) {
	for _, failRegistered := range []bool{false, true} {
		t.Run(fmt.Sprint("registered_failure=", failRegistered), func(t *testing.T) {
			up, srv, k := newTestKeeper(t)
			c := dialWS(t, srv, "Bearer lifecycle")
			other := dialWS(t, srv, "Bearer lifecycle")
			ev := wsCreate(t, c, `{"type":"response.create","input":"A","store":false}`)
			for _, text := range []string{"B", "C"} {
				old := responseID(ev)
				k.store.mu.Lock()
				k.store.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
				k.store.mu.Unlock()
				ev = wsCreate(t, c, fmt.Sprintf(`{"type":"response.create","input":%q,"store":false,"previous_response_id":%q}`, text, old))
				if ev["type"] != "response.completed" {
					t.Fatalf("private continuation lost through TTL: %v", ev)
				}
			}
			id := responseID(ev)
			foreign := wsCreate(t, other, fmt.Sprintf(`{"type":"response.create","input":"foreign","previous_response_id":%q}`, id))
			if foreign["type"] != "error" {
				t.Fatal("private history leaked across sockets")
			}
			// Existing accepted pin fits; another result cannot. Failure must preserve
			// that pin and must not publish a store:true alias as a partial transaction.
			_, _, pinned, _, _ := inspectStore(t, k.store)
			k.store.mu.Lock()
			k.store.pinMax = pinned
			k.store.mu.Unlock()
			rejected := wsCreate(t, c, fmt.Sprintf(`{"type":"response.create","input":"FAIL","store":%v,"previous_response_id":%q}`, failRegistered, id))
			if rejected["type"] != "error" || rejected["error"].(map[string]any)["code"] != "history_storage_failed" {
				t.Fatalf("replacement not rejected explicitly: %v", rejected)
			}
			inspectStore(t, k.store)
			k.store.mu.Lock()
			k.store.pinMax = 1 << 20
			k.store.mu.Unlock()
			// Transition private -> registered shares all old input/output. A failed
			// turn's input/output must NOT get appended to the accepted reference.
			ev = wsCreate(t, c, fmt.Sprintf(`{"type":"response.create","input":"D","store":true,"previous_response_id":%q}`, id))
			if ev["type"] != "response.completed" {
				t.Fatalf("transition lost accepted state: %v", ev)
			}
			got := up.last()
			if len(got) != 10 {
				t.Fatalf("history truncated/failed turn retained: len=%d want10", len(got))
			}
			raw, _ := json.Marshal(got)
			if strings.Contains(string(raw), "FAIL") {
				t.Fatal("failed replacement contaminated accepted history")
			}
			registered := responseID(ev)
			ev = wsCreate(t, other, fmt.Sprintf(`{"type":"response.create","input":"E","previous_response_id":%q}`, registered))
			if ev["type"] != "response.completed" {
				t.Fatalf("registered transition not usable elsewhere: %v", ev)
			}
			inspectStore(t, k.store)
		})
	}
}

func TestStorageTransactionFailureVisibleOnAllTransports(t *testing.T) {
	for _, transport := range []string{"json", "sse", "ws"} {
		t.Run(transport, func(t *testing.T) {
			_, srv, k := newTestKeeper(t)
			// First obtain an accepted reference, then force bbolt's actual read-only
			// transaction rejection. This is NOT an ENOSPC fixture.
			var previous string
			if transport == "ws" {
				c := dialWS(t, srv, "Bearer transaction")
				ev := wsCreate(t, c, `{"type":"response.create","input":"A","store":false}`)
				previous = responseID(ev)
				reopenReadOnly(t, k.store)
				ev = wsCreate(t, c, fmt.Sprintf(`{"type":"response.create","input":"B","previous_response_id":%q}`, previous))
				if ev["type"] != "error" || ev["error"].(map[string]any)["code"] != "history_storage_failed" {
					t.Fatalf("I/O failure success: %v", ev)
				}
				// Pin still exists and is charged; unlike expired aliases, no TTL applies.
				_, _, pinned, _, _ := inspectStore(t, k.store)
				if pinned == 0 {
					t.Fatal("previous pin lost")
				}
				k.store.mu.Lock()
				closeErr := k.store.db.Close()
				db, err := bolt.Open(k.store.path, 0600, boltOptions())
				if err == nil {
					k.store.db = db
				}
				k.store.mu.Unlock()
				if closeErr != nil {
					t.Fatal(closeErr)
				}
				if err != nil {
					t.Fatal(err)
				}
				ev = wsCreate(t, c, fmt.Sprintf(`{"type":"response.create","input":"C","store":false,"previous_response_id":%q}`, previous))
				if ev["type"] != "response.completed" {
					t.Fatalf("accepted private state lost after I/O refusal: %v", ev)
				}
				return
			}
			_, body, _ := post(t, srv, "Bearer transaction", `{"input":"A"}`)
			var first map[string]any
			if err := json.Unmarshal([]byte(body), &first); err != nil {
				t.Fatal(err)
			}
			previous = first["id"].(string)
			reopenReadOnly(t, k.store)
			status, body, _ := post(t, srv, "Bearer transaction", fmt.Sprintf(`{"input":"B","stream":%v,"previous_response_id":%q}`, transport == "sse", previous))
			if !strings.Contains(body, "history_storage_failed") || strings.Contains(body, `"status":"completed"`) {
				t.Fatalf("false terminal success: %d %s", status, body)
			}
			if transport == "json" && status != 503 {
				t.Fatalf("status=%d", status)
			}
			owner := ownerOf(&http.Request{Header: http.Header{"Authorization": []string{"Bearer transaction"}}})
			if _, ok := k.store.get(previous, owner); !ok {
				t.Fatal("accepted id lost on failed transaction")
			}
			inspectStore(t, k.store)
		})
	}
}

func TestHTTPStreamsDeltasBeforeRejectedTerminal(t *testing.T) {
	proceed := make(chan struct{})
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"hello\"}\n\n")
		w.(http.Flusher).Flush()
		select {
		case <-proceed:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_rejected\",\"object\":\"response\",\"status\":\"completed\",\"output\":[]}}\n\ndata: [DONE]\n\n")
	}))
	defer up.Close()
	u, _ := url.Parse(up.URL)
	k := newKeeper(config{upstream: u, ttl: time.Minute, maxStoreBytes: 1, maxRequestBytes: 32 << 20, storeDir: t.TempDir(), storeMetaBytes: 1 << 20, storePinBytes: 1, storeCompactBytes: 4 << 20})
	defer k.store.close()
	srv := httptest.NewServer(k)
	defer srv.Close()
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(srv.URL+"/v1/responses", "application/json", strings.NewReader(`{"input":"A","stream":true}`))
	if err != nil {
		close(proceed)
		t.Fatal(err)
	}
	defer resp.Body.Close()
	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadString('\n')
	close(proceed)
	if err != nil || !strings.Contains(first, "response.output_text.delta") {
		t.Fatalf("delta buffered until terminal: %q %v", first, err)
	}
	rest, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(rest), "history_storage_failed") || strings.Contains(string(rest), "response.completed") || strings.Contains(string(rest), "[DONE]") {
		t.Fatalf("false terminal: %s", rest)
	}
}

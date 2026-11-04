package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

type ticketUpstream struct {
	mu      sync.Mutex
	headers []string // ticket header per forwarded POST
	tails   []string // first bytes of each forwarded WS frame
	fake    fakeUpstream
}

func gateKeeper(t *testing.T, mem int64) (*httptest.Server, *keeper, *ticketUpstream) {
	gu := &ticketUpstream{}
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v1/sub2api/billing":
			return
		case isUpgradeRequest(r):
			c, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer c.CloseNow()
			c.SetReadLimit(-1)
			_, b, err := c.Read(r.Context())
			if err == nil {
				gu.mu.Lock()
				tail := string(b[:min(len(b), 200)])
				if strings.Contains(string(b), "forged") {
					tail += " FORGED"
				}
				gu.tails = append(gu.tails, tail)
				gu.mu.Unlock()
			}
			_, _, _ = c.Read(r.Context())
			return
		}
		gu.mu.Lock()
		gu.headers = append(gu.headers, r.Header.Get(ticketHeader))
		gu.mu.Unlock()
		gu.fake.ServeHTTP(w, r)
	}))
	t.Cleanup(up.Close)
	u, _ := url.Parse(up.URL)
	_, srv, k := newTestKeeper(t, func(c *config) {
		c.upstream, c.maxRequestBytes = u, 96<<20
		c.maxStoreBytes, c.storePinBytes, c.storeMetaBytes, c.storeCompactBytes = 1<<30, 256<<20, 32<<20, 1<<30
	})
	k.gate.diskFree = func() (int64, error) { return 1 << 40, nil }
	k.gate.memAvailable = func() (int64, error) { return mem, nil }
	return srv, k, gu
}

func waitLedgerEmpty(t *testing.T, k *keeper) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m, _ := filepath.Glob(filepath.Join(k.gate.dir, "*"+ticketSuffix))
		if len(m) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("ledger not empty: %v", m)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// A 64 MiB body is admitted and its ticket id goes to the inner hop; a small
// body carries no ticket, and a client-supplied ticket header is stripped.
func TestGateHTTPTicketPropagatedAndReleased(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	if status, out := postBody(t, srv, strings.NewReader(largeBody(int(largeBodyBytes))), largeBodyBytes); status != 200 {
		t.Fatalf("large: %d %.200s", status, out)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(`{"model":"m","input":"hi"}`))
	req.Header.Set(ticketHeader, strings.Repeat("f", 32))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	gu.mu.Lock()
	defer gu.mu.Unlock()
	if len(gu.headers) != 2 || len(gu.headers[0]) != 32 || gu.headers[1] != "" {
		t.Fatalf("forwarded tickets: %q", gu.headers)
	}
	waitLedgerEmpty(t, k)
	waitWorking(t, k, 0)
}

// Refusals happen before anything is sent downstream.
func TestGateHTTPRefusals(t *testing.T) {
	srv, k, gu := gateKeeper(t, 300*mib)
	status, out := postBody(t, srv, strings.NewReader(largeBody(int(largeBodyBytes))), largeBodyBytes)
	if status != 503 || !strings.Contains(out, "insufficient_memory") {
		t.Fatalf("memory: %d %.300s", status, out)
	}
	k.gate.memAvailable = func() (int64, error) { return 901 * mib, nil }
	held, e := k.gate.admit(256 * mib)
	if e != nil {
		t.Fatal(e)
	}
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/responses", strings.NewReader(largeBody(int(largeBodyBytes))))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 503 || resp.Header.Get("Retry-After") == "" {
		t.Fatalf("busy: %d %v", resp.StatusCode, resp.Header)
	}
	held.unref()
	waitLedgerEmpty(t, k)
	gu.mu.Lock()
	defer gu.mu.Unlock()
	if len(gu.headers) != 0 {
		t.Fatalf("a refused request reached the inner hop: %q", gu.headers)
	}
}

// A WS message of 64 MiB is admitted and its rebuilt frame carries the
// ticket member; a client-supplied member is dropped.
func TestGateWSTicketMember(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	c := dialWS(t, srv, "Bearer big")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame := `{"type":"response.create","ai_gateway_capacity_ticket":"forged",` + largeBody(int(largeBodyBytes))[1:]
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		gu.mu.Lock()
		n := len(gu.tails)
		gu.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	gu.mu.Lock()
	tails := append([]string(nil), gu.tails...)
	gu.mu.Unlock()
	// Bridge contract: the ticket is the FIRST member of the frame object,
	// so the bridge can lease it from a small prefix before spooling.
	if len(tails) != 1 || !strings.HasPrefix(tails[0], `{"ai_gateway_capacity_ticket":"`) ||
		tails[0][len(`{"ai_gateway_capacity_ticket":"`)+32:][:2] != `",` || strings.Contains(tails[0], "FORGED") {
		t.Fatalf("frame heads: %q", tails)
	}
	c.Close(websocket.StatusNormalClosure, "")
	waitLedgerEmpty(t, k)
}

// An unticketed frame is sent byte-for-byte without the reserved prefix.
func TestGateWSUnticketedFrameUnchanged(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	c := dialWS(t, srv, "Bearer big")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.create","model":"m","input":"small"}`)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		gu.mu.Lock()
		n := len(gu.tails)
		gu.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	gu.mu.Lock()
	head := append([]string(nil), gu.tails...)
	gu.mu.Unlock()
	if len(head) != 1 || !strings.HasPrefix(head[0], `{"model":"m"`) {
		t.Fatalf("unticketed frame head: %q", head)
	}
	c.Close(websocket.StatusNormalClosure, "")
	waitLedgerEmpty(t, k)
}

// Two 40 MiB deltas: the second request is small, but its expanded history
// reaches 64 MiB, so keeper authenticates and admits it before sending.
func TestGateHTTPExpansionAdmitted(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	text := strings.Repeat("x", 40<<20)
	status, body, _ := post(t, srv, "Bearer big", `{"model":"m","stream":true,"input":"`+text+`"}`)
	if status != 200 {
		t.Fatalf("turn1 %d %.200s", status, body)
	}
	id1 := completedID(t, body)
	status, body, _ = post(t, srv, "Bearer big", `{"model":"m","stream":true,"previous_response_id":"`+id1+`","input":"`+text+`"}`)
	if status != 200 {
		t.Fatalf("turn2 %d %.200s", status, body)
	}
	gu.mu.Lock()
	hdrs := append([]string(nil), gu.headers...)
	gu.mu.Unlock()
	if len(hdrs) != 2 || hdrs[0] != "" || len(hdrs[1]) != 32 {
		t.Fatalf("tickets: %q", hdrs)
	}
	waitLedgerEmpty(t, k)
}

// A native-parent frame of 64 MiB is relayed verbatim, but still carries
// its ticket as the first member.
func TestGateWSNativeRelayTicketFirst(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	c := dialWS(t, srv, "Bearer big")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	frame := `{"type":"response.create","previous_response_id":"resp_native",` + largeBody(int(largeBodyBytes))[1:]
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		gu.mu.Lock()
		n := len(gu.tails)
		gu.mu.Unlock()
		if n > 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	gu.mu.Lock()
	head := append([]string(nil), gu.tails...)
	gu.mu.Unlock()
	want := `","type":"response.create","previous_response_id":"resp_native"`
	if len(head) != 1 || !strings.HasPrefix(head[0], `{"ai_gateway_capacity_ticket":"`) || !strings.Contains(head[0], want) {
		t.Fatalf("relayed head: %q", head)
	}
	c.Close(websocket.StatusNormalClosure, "")
	waitLedgerEmpty(t, k)
}

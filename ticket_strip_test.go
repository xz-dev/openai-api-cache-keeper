package main

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func firstUpstreamFrame(t *testing.T, gu *ticketUpstream, want int) string {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		gu.mu.Lock()
		n := len(gu.tails)
		var tail string
		if n >= want {
			tail = gu.tails[want-1]
		}
		gu.mu.Unlock()
		if n >= want {
			return tail
		}
		if time.Now().After(deadline) {
			t.Fatalf("upstream saw %d frames, want %d", n, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// M4: keeper is the only hop that strips a client ticket member (D4), so it
// strips it from every client frame, including frames relayed verbatim.
func TestClientTicketMemberStrippedFromRelayedFrames(t *testing.T) {
	srv, k, gu := gateKeeper(t, 901*mib)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	frames := []struct{ send, head string }{
		// a native-parent create, relayed verbatim
		{`{"ai_gateway_capacity_ticket":"forged","type":"response.create","previous_response_id":"resp_native","model":"m","input":"x"}`,
			`{"type":"response.create","previous_response_id":"resp_native","model":"m","input":"x"}`},
		// a non-create frame, member last
		{`{"type":"response.cancel", "ai_gateway_capacity_ticket" : "forged" }`, `{"type":"response.cancel" }`},
		// escaped key, the only member besides type
		{`{"ai_gateway_capacity_\u0074icket":"forged","type":"x"}`, `{"type":"x"}`},
	}
	for i, f := range frames {
		c := dialWS(t, srv, "Bearer big")
		if err := c.Write(ctx, websocket.MessageText, []byte(f.send)); err != nil {
			t.Fatal(err)
		}
		if got := firstUpstreamFrame(t, gu, i+1); got != f.head {
			t.Fatalf("frame %d forwarded as %q, want %q", i, got, f.head)
		}
		c.Close(websocket.StatusNormalClosure, "")
	}
	// A ticketed verbatim relay carries keeper's ticket first and no forged member.
	c := dialWS(t, srv, "Bearer big")
	frame := `{"type":"response.create","previous_response_id":"resp_native","ai_gateway_capacity_ticket":"forged",` + largeBody(int(largeBodyBytes))[1:]
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	got := firstUpstreamFrame(t, gu, len(frames)+1)
	if !strings.HasPrefix(got, `{"ai_gateway_capacity_ticket":"`) || strings.Contains(got, "FORGED") || strings.Count(got, ticketMember) != 1 {
		t.Fatalf("ticketed relay head: %q", got)
	}
	c.Close(websocket.StatusNormalClosure, "")
	waitLedgerEmpty(t, k)
}

// M4: the ticket header is stripped on native HTTP paths too.
func TestClientTicketHeaderStrippedOnNativePath(t *testing.T) {
	srv, _, gu := gateKeeper(t, 901*mib)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(`{"model":"m","messages":[]}`))
	req.Header.Set(ticketHeader, strings.Repeat("f", 32))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	gu.mu.Lock()
	defer gu.mu.Unlock()
	if len(gu.headers) != 1 || gu.headers[0] != "" {
		t.Fatalf("forwarded ticket headers: %q", gu.headers)
	}
}

// Minor 7: a message of unknown length is admitted at the cap once 64 MiB
// have arrived; when the whole message is spooled, its ticket (record and
// ledger file) is lowered to the real size, so a WS frame relayed verbatim
// does not stay reserved at the cap.
func TestUnknownLengthTicketLoweredToRealSize(t *testing.T) {
	_, k, _ := gateKeeper(t, 901*mib)
	ctx, scope := withGateScope(context.Background(), k.gate)
	admit := func(context.Context) error {
		if e := scope.ensure(k.cfg.maxRequestBytes); e != nil {
			return &authRefusal{e}
		}
		return nil
	}
	body := largeBody(int(largeBodyBytes) + 1000)
	f, err := k.spoolRequest(ctx, strings.NewReader(body), admit)
	scope.done()
	if err != nil {
		t.Fatal(err)
	}
	if f.ticket == nil || f.ticket.rec.n != int64(len(body)) {
		t.Fatalf("ticket %+v, want n=%d", f.ticket, len(body))
	}
	raw, err := os.ReadFile(k.gate.ticketPath(f.ticket.id()))
	if err != nil {
		t.Fatal(err)
	}
	if rec, err := parseTicket(raw); err != nil || rec.n != int64(len(body)) {
		t.Fatalf("ledger record %+v %v, want n=%d", rec, err, len(body))
	}
	f.Close()
	waitLedgerEmpty(t, k)
}

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/coder/websocket"
)

// membersFrame builds a compact object of n members with the ticket member
// at each index in at.
func membersFrame(n int, at ...int) (frame, want string) {
	ticket := map[int]bool{}
	for _, i := range at {
		ticket[i] = true
	}
	var all, kept []string
	for i := range n {
		m := fmt.Sprintf(`"k%d":%d`, i, i)
		if ticket[i] {
			m = `"` + ticketMember + `":"forged"`
		} else {
			kept = append(kept, m)
		}
		all = append(all, m)
	}
	return "{" + strings.Join(all, ",") + "}", "{" + strings.Join(kept, ",") + "}"
}

// Re-review 3: the ticket member is stripped wherever it appears, with no
// member cap; the 256-member scan cap used to relay a member after 256.
func TestStripTicketMemberEveryPosition(t *testing.T) {
	_, _, k := newTestKeeper(t)
	type tc struct{ name, frame, want string }
	var cases []tc
	for _, at := range [][]int{{0}, {150}, {255}, {256}, {257}, {299}, {0, 1}, {298, 299}, {0, 299}, {10, 11, 12, 280}} {
		f, w := membersFrame(300, at...)
		cases = append(cases, tc{fmt.Sprint(at), f, w})
	}
	only, _ := membersFrame(1, 0)
	twoOnly, _ := membersFrame(2, 0, 1)
	cases = append(cases,
		tc{"only", only, "{}"},
		tc{"two only", twoOnly, "{}"},
		tc{"spaces last", `{ "type":"x" , "ai_gateway_capacity_ticket" : "f" }`, `{ "type":"x" }`},
		tc{"spaces first", "{\n \"ai_gateway_capacity_ticket\":\"f\" ,\n \"type\":\"x\"\n}", "{\n \"type\":\"x\"\n}"},
		tc{"escaped key", `{"a":1,"ai_gateway_capacity_\u0074icket":"f","b":[1,{"c":2}]}`, `{"a":1,"b":[1,{"c":2}]}`},
	)
	for _, c := range cases {
		sp, err := k.spoolRequest(context.Background(), strings.NewReader(c.frame), nil)
		if err != nil {
			t.Fatal(err)
		}
		found, err := ticketMemberIn(sp)
		if err != nil || !found {
			t.Fatalf("%s: found=%t err=%v", c.name, found, err)
		}
		rc := stripTicketMember(sp)
		got, err := io.ReadAll(rc)
		rc.Close()
		sp.Close()
		if err != nil || string(got) != c.want {
			t.Fatalf("%s: stripped to %.300q (err %v), want %.300q", c.name, got, err, c.want)
		}
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(got, &doc); err != nil || doc[ticketMember] != nil {
			t.Fatalf("%s: stripped frame invalid or still ticketed: %v", c.name, err)
		}
	}
	// A frame keeper cannot parse as one object is refused by name.
	for _, bad := range []string{`{"a":1`, `{"a":1} {"ai_gateway_capacity_ticket":"f"}`, `[{"ai_gateway_capacity_ticket":"f"}]`, `"x"`, ``, `{"a" 1}`} {
		sp, err := k.spoolRequest(context.Background(), strings.NewReader(bad), nil)
		if err != nil {
			t.Fatal(err)
		}
		_, err = ticketMemberIn(sp)
		sp.Close()
		var refused *clientFrameError
		if !errors.As(err, &refused) || refused.apiError().code != "client_frame_unparseable" || refused.apiError().hop != keeperHop {
			t.Fatalf("%q: %v", bad, err)
		}
	}
}

// Through keeper: a forged member after member 256 never reaches the
// upstream, and an unparseable client frame is refused to the client by
// name and never relayed.
func TestWSTicketMemberPastCapAndUnparseableFrame(t *testing.T) {
	srv, _, gu := gateKeeper(t, 901*mib)
	ctx, cancel := context.WithTimeout(context.Background(), upstreamFrameWait)
	defer cancel()
	frame, _ := membersFrame(300, 299)
	frame = `{"type":"response.cancel",` + frame[1:]
	c := dialWS(t, srv, "Bearer big")
	if err := c.Write(ctx, websocket.MessageText, []byte(frame)); err != nil {
		t.Fatal(err)
	}
	if got := firstUpstreamFrame(t, gu, 1, "the 300-member frame"); strings.Contains(got, "FORGED") || !strings.HasPrefix(got, `{"type":"response.cancel","k0":0,`) {
		t.Fatalf("300-member frame forwarded as %q", got)
	}
	c.Close(websocket.StatusNormalClosure, "")

	c = dialWS(t, srv, "Bearer big")
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.cancel"} {"ai_gateway_capacity_ticket":"forged"}`)); err != nil {
		t.Fatal(err)
	}
	_, ev, err := c.Read(ctx)
	if err != nil || !strings.Contains(string(ev), `"code":"client_frame_unparseable"`) || !strings.Contains(string(ev), `"hop":"keeper"`) {
		t.Fatalf("unparseable frame: event %s err %v", ev, err)
	}
	// The socket stays usable; the next frame is the first the upstream records.
	if err := c.Write(ctx, websocket.MessageText, []byte(`{"type":"response.cancel"}`)); err != nil {
		t.Fatal(err)
	}
	if got := firstUpstreamFrame(t, gu, 2, "the frame after the refused one"); got != `{"type":"response.cancel"}` {
		t.Fatalf("after the refused frame the upstream recorded %q", got)
	}
	c.Close(websocket.StatusNormalClosure, "")
}

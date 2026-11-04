package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
)

func TestPreparedModelFirst(t *testing.T) {
	for _, ws := range []bool{false, true} {
		for _, tc := range []struct {
			name, fields, want string
		}{
			{"plain", `,"model":"m"`, `{"model":"m","input":[],"a":1,"z":2}`},
			{"escaped-unicode", `,"model":"\u006d/雪\"\\"`, `{"model":"\u006d/雪\"\\","input":[],"a":1,"z":2}`},
			{"absent", ``, `{"input":[],"a":1,"z":2}`},
			{"duplicate", `,"model":"old","mo\u0064el":"last"`, `{"model":"last","input":[],"a":1,"z":2}`},
		} {
			t.Run(fmt.Sprintf("%s/ws=%t", tc.name, ws), func(t *testing.T) {
				_, _, k := newTestKeeper(t)
				body := `{"z":2,"input":[],"a":1` + tc.fields + `}`
				if ws {
					body = `{"type":"response.create",` + body[1:]
				}
				raw, err := k.spoolRequest(context.Background(), strings.NewReader(body), nil)
				if err != nil {
					t.Fatal(err)
				}
				defer raw.Close()
				turn, apiErr := k.prepareDiskEnv(context.Background(), raw, [32]byte{}, func(string) ([32]byte, bool) { return [32]byte{}, false }, ws, ws)
				if apiErr != nil {
					t.Fatal(apiErr)
				}
				defer turn.file.Close()
				upstream, err := turn.file.requestBody()
				if err != nil {
					t.Fatal(err)
				}
				defer upstream.Close()
				got, err := io.ReadAll(upstream)
				if err != nil {
					t.Fatal(err)
				}
				want := tc.want
				if ws {
					want = strings.Replace(want, `,"z":2}`, `,"type":"response.create","z":2}`, 1)
				}
				if string(got) != want {
					t.Fatalf("got %s; want %s", got, want)
				}
			})
		}
	}
}

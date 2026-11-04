package main

import (
	"errors"
	"testing"
)

// Span rewrite keeps every unchanged byte and collapses duplicates of the
// fields it rewrites to the last-wins value, like the old decoded rewrite.
func TestRewritePayloadSplicesSpans(t *testing.T) {
	for _, tc := range []struct {
		in     string
		stored bool
		want   string
	}{
		{`{ "object" : "response", "id":"up", "status":"completed" }`, true, `{ "object" : "response", "id":"resp_up", "status":"completed","store":true }`},
		{`{"id":"a","object":"response","id":"b","store":false,"store":false}`, true, `{"object":"response","id":"resp_b","store":true}`},
		{`{"id":"a","object":"response","id":"b","store":false}`, false, `{"object":"response","id":"resp_b","store":false}`},
		{`{"object":"response","id":"\u0072esp_x"}`, false, `{"object":"response","id":"\u0072esp_x"}`},
		{`{"object":"response","id":"\u0061b"}`, false, `{"object":"response","id":"resp_\u0061b"}`},
		{`{"object":"response","id":null,"store":false}`, true, `{"object":"response","id":null,"store":false}`},
		{`{"object":"respons\u0065","id":"up"}`, true, `{"object":"respons\u0065","id":"up"}`},
		{`{"type":"response.created","response": {"id":"u"} ,"x":"<&>"}`, false, `{"type":"response.created","response": {"id":"resp_u"} ,"x":"<&>"}`},
		{`{"type":"response.custom","response":{"id":"u"}}`, true, `{"type":"response.custom","response":{"id":"u"}}`},
		{`{"type":"response.completed","response":{"id":"a"},"response":{"id":"b"}}`, false, `{"type":"response.completed","response":{"id":"resp_b"}}`},
		{`{"type":"response.completed","response":{"id":"resp_b","store":true}}`, true, `{"type":"response.completed","response":{"id":"resp_b","store":true}}`},
	} {
		got, err := rewritePayload([]byte(tc.in), tc.stored)
		if err != nil || string(got) != tc.want {
			t.Errorf("rewrite(%s, %v) = %s, %v; want %s", tc.in, tc.stored, got, err, tc.want)
		}
	}
	for _, bad := range []string{`{"object":"response","id":7}`, `{"type":"response.created","response":[]}`, `{"object":"response","id":"x"`, `{"object":"response"} {}`} {
		if _, err := rewritePayload([]byte(bad), true); !errors.Is(err, errUpstreamData) {
			t.Errorf("rewrite(%s) err=%v, want errUpstreamData", bad, err)
		}
	}
}

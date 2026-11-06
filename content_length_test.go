package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

// upstreamPost is what the upstream saw of one forwarded POST.
type upstreamPost struct {
	path     string
	declared int64
	te       []string
	body     int
}

// Exit gate 7.5: keeper forwards a spooled body with Content-Length equal to
// the bytes it sends, not chunked, so the upstream can charge the whole body
// up front. Covers the HTTP path (plain and with an expanded parent) and the
// WS-to-HTTP path.
func TestForwardedBodyDeclaresContentLength(t *testing.T) {
	var mu sync.Mutex
	var posts []upstreamPost
	fake := &fakeUpstream{}
	rec := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		posts = append(posts, upstreamPost{r.URL.Path, r.ContentLength, r.TransferEncoding, len(body)})
		mu.Unlock()
		r.Body = io.NopCloser(bytes.NewReader(body))
		fake.ServeHTTP(w, r)
	}))
	t.Cleanup(rec.Close)
	useRec := func(c *config) { c.upstream, _ = url.Parse(rec.URL) }
	_, srv, _ := newTestKeeper(t, useRec)

	status, sse, _ := post(t, srv, "Bearer a", `{"model":"m","stream":true,"input":[{"role":"user","content":"hello"}]}`)
	if status != http.StatusOK {
		t.Fatalf("first turn: %d %s", status, sse)
	}
	parent := completedID(t, sse)
	child := `{"model":"m","stream":true,"previous_response_id":"` + parent + `","input":[{"role":"user","content":"again"}]}`
	if status, out, _ := post(t, srv, "Bearer a", child); status != http.StatusOK {
		t.Fatalf("expanded turn: %d %s", status, out)
	}
	// A WS turn is converted to an HTTP POST by the local-turn handler.
	_, local, _ := newLocalTestKeeper(t, useRec)
	conn := dialWS(t, local, "Bearer a")
	if ev := wsCreate(t, conn, `{"type":"response.create","model":"m","input":[{"role":"user","content":"ws"}]}`); ev["type"] != "response.completed" {
		t.Fatalf("ws turn: %v", ev)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(posts) != 3 {
		t.Fatalf("upstream saw %d POSTs, want 3", len(posts))
	}
	if posts[1].body <= len(child) {
		t.Fatalf("expanded body %d bytes is not larger than the client body %d", posts[1].body, len(child))
	}
	for i, p := range posts {
		if p.declared != int64(p.body) || len(p.te) != 0 || !strings.HasSuffix(p.path, "/responses") {
			t.Fatalf("POST %d %s: Content-Length %d, Transfer-Encoding %v, body %d bytes", i, p.path, p.declared, p.te, p.body)
		}
	}
}

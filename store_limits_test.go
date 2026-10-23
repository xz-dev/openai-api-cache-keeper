package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLoadConfigStoreLimits(t *testing.T) {
	for _, name := range []string{"ROUTING_INDEX_URL", "UPSTREAM_CPA_URL", "UPSTREAM_AISIX_URL", "CPA_API_KEY", "STORE_TTL", "STORE_MAX_ENTRIES", "STORE_MAX_BYTES"} {
		t.Setenv(name, "")
	}
	t.Setenv("UPSTREAM_URL", "http://upstream:8080")
	for _, tc := range []struct {
		name, entries, bytes   string
		wantEntries, wantBytes int64
		invalid                bool
	}{
		{name: "defaults", wantEntries: 100},
		{name: "unlimited", entries: "0", bytes: "0"},
		{name: "bounded", entries: "10000", bytes: "12345", wantEntries: 10000, wantBytes: 12345},
		{name: "negative entries", entries: "-1", invalid: true},
		{name: "negative bytes", bytes: "-1", invalid: true},
		{name: "invalid entries", entries: "1.5", invalid: true},
		{name: "overflow bytes", bytes: "9223372036854775808", invalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("STORE_MAX_ENTRIES", tc.entries)
			t.Setenv("STORE_MAX_BYTES", tc.bytes)
			cfg, err := loadConfig()
			if (err != nil) != tc.invalid {
				t.Fatalf("loadConfig error=%v", err)
			}
			if err == nil && (cfg.maxStoreEntries != tc.wantEntries || cfg.maxStoreBytes != tc.wantBytes || cfg.ttl != 10*time.Minute) {
				t.Fatalf("limits entries=%d bytes=%d ttl=%s", cfg.maxStoreEntries, cfg.maxStoreBytes, cfg.ttl)
			}
		})
	}
	// Unlimited applies only to the two retained-cache limits.
	t.Setenv("STORE_MAX_ENTRIES", "0")
	t.Setenv("STORE_MAX_BYTES", "0")
	t.Setenv("MAX_REQUEST_BYTES", "0")
	if _, err := loadConfig(); err == nil {
		t.Fatal("zero request safety limit accepted")
	}
}

func TestStoreEntryLimitEvictsLeastRecentlyUsed(t *testing.T) {
	s := newTestStore(t, time.Minute, 0, 1<<20, 1<<19, 4<<20)
	s.maxEntries = 2
	o := [32]byte{1}
	now := time.Unix(1, 0)
	s.now = func() time.Time { return now }
	for _, id := range []string{"z-oldest", "a-second"} {
		mustReviewPut(t, s, id, o, "", items(fmt.Sprintf(`%q`, id)))
		now = now.Add(time.Second)
	}
	// Reading updates LRU order, but not the alias's creation-based TTL.
	roundtrip(t, s, "z-oldest", o, []string{`"z-oldest"`})
	mustReviewPut(t, s, "m-newest", o, "", items(`"new"`))
	if _, ok := s.get("a-second", o); ok {
		t.Fatal("least-recently-used alias survived entry pressure")
	}
	roundtrip(t, s, "z-oldest", o, []string{`"z-oldest"`})
	roundtrip(t, s, "m-newest", o, []string{`"new"`})
	// Replacing an alias does not consume another entry.
	mustReviewPut(t, s, "m-newest", o, "", items(`"replaced"`))
	if entries, _ := s.stats(); entries != 2 {
		t.Fatalf("replacement entries=%d", entries)
	}
	now = now.Add(2 * time.Minute)
	mustReviewPut(t, s, "after-expiry", o, "", items(`"fresh"`))
	if entries, _ := s.stats(); entries != 1 {
		t.Fatalf("expired aliases survived: %d", entries)
	}
	inspectStore(t, s)
}

func TestStoreEntryLimitProtectedOnlyTerminates(t *testing.T) {
	for _, tc := range []struct {
		name    string
		bytes   int64
		wantErr error
		wantN   int
	}{
		{"one protected alias fits", 0, nil, 1},
		{"only protected alias exceeds byte budget", 1, errStoreBudget, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t, time.Hour, tc.bytes, 1<<20, 1<<19, 4<<20)
			s.maxEntries = 1
			if _, err := s.put("incoming", [32]byte{1}, "", items(`"X"`)); err != tc.wantErr {
				t.Fatalf("protected admission error=%v want=%v", err, tc.wantErr)
			}
			if entries, _ := s.stats(); entries != tc.wantN {
				t.Fatalf("aliases=%d want=%d", entries, tc.wantN)
			}
			inspectStore(t, s)
		})
	}
}

func TestStoreLimitsIndependent(t *testing.T) {
	for _, tc := range []struct {
		name           string
		entries, bytes int64
		want           int
	}{
		{"both unlimited", 0, 0, 3},
		{"entries only", 2, 0, 2},
		{"bytes only", 0, 10, 1},
		{"bytes tighter", 2, 10, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestStore(t, time.Hour, tc.bytes, 1<<20, 1<<19, 4<<20)
			s.maxEntries = tc.entries
			o := [32]byte{1}
			now := time.Unix(1, 0)
			s.now = func() time.Time { return now }
			for _, id := range []string{"a", "b", "c"} {
				mustReviewPut(t, s, id, o, "", items(fmt.Sprintf(`"%s1234"`, id)))
				now = now.Add(time.Second)
			}
			if n, _ := s.stats(); n != tc.want {
				t.Fatalf("entries=%d want=%d", n, tc.want)
			}
			roundtrip(t, s, "c", o, []string{`"c1234"`})
			inspectStore(t, s)
		})
	}
}

func TestStoreUnlimitedEntriesAndWriteBasedTTL(t *testing.T) {
	s := newTestStore(t, time.Minute, 0, 1<<20, 1<<19, 4<<20)
	o := [32]byte{1}
	now := time.Unix(1, 0)
	s.now = func() time.Time { return now }
	for i := 0; i < 110; i++ {
		mustReviewPut(t, s, fmt.Sprint(i), o, "", items(`"shared"`))
	}
	if entries, _ := s.stats(); entries != 110 {
		t.Fatalf("zero entries limit capped aliases: %d", entries)
	}
	now = now.Add(59 * time.Second)
	roundtrip(t, s, "0", o, []string{`"shared"`})
	now = now.Add(2 * time.Second)
	if _, ok := s.get("0", o); ok {
		t.Fatal("reading extended the write-based TTL")
	}
	if err := expireTestStore(s, now); err != nil {
		t.Fatal(err)
	}
	if entries, live := s.stats(); entries != 0 || live != 0 {
		t.Fatalf("expired unlimited cache entries=%d live=%d", entries, live)
	}
	inspectStore(t, s)
}

func TestStoreUnlimitedBytesKeepsPinSafetyBudget(t *testing.T) {
	s := newTestStore(t, time.Hour, 0, 0, 0, 0)
	s.maxEntries = 1
	if s.pinMax != 100<<20 || s.maxMeta != 32<<20 || s.compactAt != 400<<20 {
		t.Fatalf("safety budgets pin=%d meta=%d compact=%d", s.pinMax, s.maxMeta, s.compactAt)
	}
	o := [32]byte{1}
	_, pin, err := s.accept("old", o, sliceItems(items(`"pinned"`)), "", true, true)
	if err != nil {
		t.Fatal(err)
	}
	defer s.release(pin)
	mustReviewPut(t, s, "new", o, "", items(`"new"`))
	if _, ok := s.get("old", o); ok {
		t.Fatal("old alias not evicted")
	}
	if got, ok := s.getPinned(pin, o); !ok || string(got[0]) != `"pinned"` {
		t.Fatal("entry eviction broke an active socket pin")
	}
	inspectStore(t, s)
}

func TestKeeperEntryLimit(t *testing.T) {
	_, srv, k := newTestKeeper(t, func(c *config) { c.maxStoreEntries = 1 })
	for _, input := range []string{"first", "second"} {
		_, body, _ := post(t, srv, "Bearer owner", fmt.Sprintf(`{"model":"m","input":%q,"stream":true}`, input))
		completedID(t, body)
		if completedStore(t, body) != true {
			t.Fatal("entry cap rejected the incoming turn")
		}
	}
	if entries, _ := k.store.stats(); entries != 1 {
		t.Fatalf("configured entry cap not applied: %d", entries)
	}
}

func TestUnlimitedBytesHTTPWSAndBackground(t *testing.T) {
	up, srv, k := newWSNativeKeeper(t, func(c *config) {
		c.maxStoreBytes = 0
		c.maxStoreEntries = 100
	})
	_, body, _ := post(t, srv, "Bearer owner", `{"model":"m","input":"A","stream":true}`)
	id := completedID(t, body)
	c := dialWS(t, srv, "Bearer owner")
	event := wsCreate(t, c, `{"type":"response.create","model":"m","input":"B","previous_response_id":"`+id+`"}`)
	if event["type"] != "response.completed" {
		t.Fatalf("WS continuation failed: %v", event)
	}
	if got := strings.Join(kinds(up.last()), ","); got != "user:A,reasoning,assistant,user:B" {
		t.Fatalf("expanded history=%s", got)
	}
	status, body, _ := post(t, srv, "Bearer owner", `{"model":"m","input":"C","stream":true,"background":true}`)
	if status != http.StatusOK {
		t.Fatalf("background status=%d", status)
	}
	bgID := completedID(t, body)
	if completedStore(t, body) != true {
		t.Fatal("background history not retained with unlimited bytes")
	}
	// The background event log must also be retained, not rejected against zero.
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/v1/responses/"+bgID+"?stream=true", nil)
	req.Header.Set("Authorization", "Bearer owner")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var terminal bool
	if err := readSSE(resp.Body, nil, func(raw []byte) error {
		var e struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			return err
		}
		terminal = terminal || e.Type == "response.completed"
		return nil
	}); err != nil || !terminal {
		t.Fatalf("background replay terminal=%v err=%v", terminal, err)
	}
	if entries, _ := k.store.stats(); entries != 3 {
		t.Fatalf("retained aliases=%d", entries)
	}
}

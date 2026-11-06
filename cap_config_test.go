package main

import (
	"bufio"
	"encoding/json"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// capEnv clears the routed settings and points STORE_DIR at a temp dir, so
// loadConfig reads only MAX_REQUEST_BYTES and STORE_PIN_MAX_BYTES.
func capEnv(t *testing.T, capValue string) {
	t.Helper()
	for _, name := range []string{"ROUTING_INDEX_URL", "UPSTREAM_CPA_URL", "UPSTREAM_AISIX_URL", "CPA_API_KEY", "STORE_MAX_BYTES", "STORE_PIN_MAX_BYTES", "CAPACITY_LEDGER_DIR", "CAPACITY_MEMINFO"} {
		t.Setenv(name, "")
	}
	t.Setenv("UPSTREAM_URL", "http://127.0.0.1:1")
	t.Setenv("STORE_DIR", t.TempDir())
	t.Setenv("MAX_REQUEST_BYTES", capValue)
}

// Without MAX_REQUEST_BYTES every limit derived from the cap is the
// production 256 MiB, and the error limit_bytes values are unchanged.
func TestCapDefaultUnchanged(t *testing.T) {
	capEnv(t, "")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	const capBytes = 268435456
	if cfg.capBytes != capBytes || cfg.maxRequestBytes != capBytes || cfg.storePinBytes != capBytes {
		t.Fatalf("defaults cap=%d request=%d pin=%d", cfg.capBytes, cfg.maxRequestBytes, cfg.storePinBytes)
	}
	k := newKeeper(cfg)
	t.Cleanup(func() { _ = k.store.close() })
	if k.work.historyMax != capBytes || k.work.spoolMax != 4*capBytes || k.store.pinMax != capBytes {
		t.Fatalf("derived history=%d spool=%d pin=%d", k.work.historyMax, k.work.spoolMax, k.store.pinMax)
	}
	for _, tc := range []struct {
		err  error
		code string
	}{
		{historySizeError(k.work.historyMax), "history_too_large"},
		{requestSizeError(k.cfg.maxRequestBytes), "request_too_large"},
	} {
		if e := workingError(tc.err); e.limit != capBytes || e.code != tc.code || e.status != 413 {
			t.Fatalf("%s: status=%d code=%s limit=%d", tc.code, e.status, e.code, e.limit)
		}
	}

	// A declared 256 MiB + 1 body gets the named 413 at the headers.
	srv := httptest.NewServer(k)
	t.Cleanup(srv.Close)
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	_, _ = io.WriteString(conn, "POST /v1/responses HTTP/1.1\r\nHost: keeper\r\nAuthorization: Bearer a\r\nContent-Type: application/json\r\nContent-Length: 268435457\r\n\r\n")
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body struct {
		Error struct {
			Code       string `json:"code"`
			Hop        string `json:"hop"`
			LimitBytes int64  `json:"limit_bytes"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 413 || body.Error.Code != "request_too_large" || body.Error.Hop != keeperHop || body.Error.LimitBytes != capBytes {
		t.Fatalf("256 MiB + 1: status=%d error=%+v", resp.StatusCode, body.Error)
	}
}

// MAX_REQUEST_BYTES is the one cap: the body limit, the history limit, the
// spool budget and the default pin budget follow it.
func TestCapConfiguredDerivesLimits(t *testing.T) {
	capEnv(t, "1073741824")
	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.capBytes != 1<<30 || cfg.maxRequestBytes != 1<<30 || cfg.storePinBytes != 1<<30 {
		t.Fatalf("cap=%d request=%d pin=%d", cfg.capBytes, cfg.maxRequestBytes, cfg.storePinBytes)
	}
	t.Setenv("STORE_PIN_MAX_BYTES", "12345") // an explicit pin budget still wins
	if cfg, err = loadConfig(); err != nil || cfg.storePinBytes != 12345 || cfg.capBytes != 1<<30 {
		t.Fatalf("pin override: pin=%d cap=%d err=%v", cfg.storePinBytes, cfg.capBytes, err)
	}
}

// Invalid caps fail at startup: below 16 MiB in loadConfig, and above what
// the disk and memory gates can ever admit in the gate check.
func TestCapInvalidFailsAtStartup(t *testing.T) {
	for _, v := range []string{"0", "-1", "abc", "16777215", "9223372036854775808"} {
		capEnv(t, v)
		if _, err := loadConfig(); err == nil {
			t.Fatalf("MAX_REQUEST_BYTES=%s accepted", v)
		}
	}
	capEnv(t, "16777216")
	if cfg, err := loadConfig(); err != nil || cfg.capBytes != 16<<20 {
		t.Fatalf("16 MiB refused: %v", err)
	}

	const gib = int64(1) << 30
	for _, tc := range []struct {
		name                string
		capBytes, disk, mem int64
		wantErrContain      string
	}{
		{name: "1 GiB on a nightly runner", capBytes: gib, disk: 12 * gib, mem: 1733 * mib},
		{name: "256 MiB on production", capBytes: 256 * mib, disk: 3 * gib, mem: 669 * mib},
		{name: "disk can never hold 7 N", capBytes: gib, disk: 7*gib - 1, mem: 64 * gib, wantErrContain: "disk footprint"},
		{name: "memory can never hold R_mem", capBytes: gib, disk: 64 * gib, mem: 900 * mib, wantErrContain: "memory footprint"},
		{name: "overflow", capBytes: math.MaxInt64, disk: math.MaxInt64, mem: math.MaxInt64, wantErrContain: "disk footprint"},
		{name: "no disk at all", capBytes: 16 * mib, disk: -gib, mem: gib, wantErrContain: "disk footprint"},
	} {
		err := capFits(tc.capBytes, tc.disk, tc.mem)
		if (err != nil) != (tc.wantErrContain != "") || (err != nil && !strings.Contains(err.Error(), tc.wantErrContain)) {
			t.Fatalf("%s: err=%v", tc.name, err)
		}
		if err != nil && !strings.Contains(err.Error(), "MAX_REQUEST_BYTES") {
			t.Fatalf("%s: error does not name the setting: %v", tc.name, err)
		}
	}

	// The real check reads the ledger filesystem size and MemTotal: no
	// filesystem or host offers 7 x 2^50 bytes.
	g, err := newCapacityGate(t.TempDir(), "", 0, "/proc/meminfo")
	if err != nil {
		t.Fatal(err)
	}
	defer g.lock.Close()
	if err := g.checkCap(1 << 50); err == nil || !strings.Contains(err.Error(), "can never be admitted") {
		t.Fatalf("2^50 cap: %v", err)
	}
	if err := g.checkCap(16 << 20); err != nil {
		t.Fatalf("16 MiB cap: %v", err)
	}
}

// With a 1 GiB cap a 512 MiB body passes the gate arithmetic and the history
// store: a continuation whose stored history expands to just over 512 MiB is
// admitted (ticket, spool, upstream), while the default cap refuses it with
// the named 256 MiB history limit before any byte is copied.
func TestCapOneGiBAdmits512MiBHistory(t *testing.T) {
	var received, ticketN, ticketDisk atomic.Int64
	var gate atomic.Pointer[capacityGate]
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if recs, _, err := gate.Load().liveTickets(false); err == nil && len(recs) == 1 {
			ticketN.Store(recs[0].n)
			ticketDisk.Store(recs[0].rDisk)
		}
		n, _ := io.Copy(io.Discard, r.Body)
		received.Store(n)
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, gateEvent("response.created", ""))
		_, _ = io.WriteString(w, gateEvent("response.completed", ""))
	}))
	t.Cleanup(up.Close)
	item, _ := json.Marshal(map[string]string{"type": "message", "role": "user", "content": strings.Repeat("X", 1<<20)})
	full := make([]json.RawMessage, 512) // deduplicated: the store holds one copy
	for i := range full {
		full[i] = item
	}
	for _, tc := range []struct {
		name     string
		capBytes int64
	}{{"default", 0}, {"1 GiB", 1 << 30}} {
		t.Run(tc.name, func(t *testing.T) {
			_, srv, k := newTestKeeper(t, func(c *config) {
				c.upstream, _ = c.upstream.Parse(up.URL)
				c.ttl = time.Hour
				c.capBytes = tc.capBytes
				c.maxRequestBytes = max(tc.capBytes, defaultCapBytes)
				c.maxStoreBytes, c.storePinBytes, c.storeMetaBytes, c.storeCompactBytes = 1<<30, 1<<30, 32<<20, 1<<30
			})
			// The nightly stack: a 12 GiB runner under the production-RAM cgroup.
			k.gate.diskFree = func() (int64, error) { return 12 << 30, nil }
			k.gate.memAvailable = func() (int64, error) { return 1965 * mib, nil }
			gate.Store(k.gate)
			mustReviewPut(t, k.store, "resp_cap_seed", [32]byte{}, "", full)
			received.Store(0)
			ticketN.Store(0)
			status, out, _ := post(t, srv, "Bearer a", turnBody("resp_cap_seed", "next"))
			if tc.capBytes == 0 {
				if status != 413 || !strings.Contains(out, `"history_too_large"`) || !strings.Contains(out, `"limit_bytes":268435456`) || received.Load() != 0 {
					t.Fatalf("default cap: status=%d upstream=%d body=%.300s", status, received.Load(), out)
				}
				return
			}
			if status != 200 || !strings.Contains(out, "response.completed") {
				t.Fatalf("1 GiB cap: status=%d body=%.300s", status, out)
			}
			if got := received.Load(); got < 512<<20 || got > 1<<30 {
				t.Fatalf("upstream received %d bytes", got)
			}
			// The gate admitted a ticket sized from the expanded history
			// (R_disk = 7 N) and released it when the turn ended.
			if n := ticketN.Load(); n < 512<<20 || ticketDisk.Load() != kDiskTotal*n {
				t.Fatalf("ticket N=%d R_disk=%d", n, ticketDisk.Load())
			}
			waitSpoolsClosed(t, k)
			waitLedgerEmpty(t, k)
		})
	}
}

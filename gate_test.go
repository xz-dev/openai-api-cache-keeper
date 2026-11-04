package main

import (
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

const mib = int64(1) << 20

// testGate is a ledger with fixed host facts: free disk and MemAvailable.
func testGate(t *testing.T, dir string, disk, mem int64) *capacityGate {
	t.Helper()
	g, err := newCapacityGate(dir, filepath.Join(dir, "no-store.db"), 1<<30, "/proc/meminfo")
	if err != nil {
		t.Fatal(err)
	}
	g.diskFree = func() (int64, error) { return disk, nil }
	g.memAvailable = func() (int64, error) { return mem, nil }
	t.Cleanup(func() { g.waiters.Wait(); g.lock.Close() })
	return g
}

func ticketFiles(t *testing.T, dir string) []string {
	m, _ := filepath.Glob(filepath.Join(dir, "*"+ticketSuffix))
	return m
}

func TestFootprintMatchesDesign(t *testing.T) {
	rd, rm := footprint(256 * mib)
	if rd != 7*256*mib {
		t.Fatalf("R_disk=%d", rd)
	}
	// D4 fit table: 404.8 + 195.2 = 600 MiB at 256 MiB.
	if d := rm - 600*mib; d < 0 || d > 16 {
		t.Fatalf("R_mem=%d (%.2f MiB)", rm, float64(rm)/float64(mib))
	}
}

// Today's host (901 MiB MemAvailable, 4.66 GB free, empty 1 GiB store)
// admits one 256 MiB ticket; a second is capacity_busy with Retry-After.
func TestGateAdmitsOneOnTodaysHost(t *testing.T) {
	dir := t.TempDir()
	g := testGate(t, dir, 4_660_000_000, 901*mib)
	t1, e := g.admit(256 * mib)
	if e != nil {
		t.Fatalf("first: %+v", e)
	}
	files := ticketFiles(t, dir)
	if len(files) != 1 {
		t.Fatalf("tickets: %v", files)
	}
	st, _ := os.Stat(files[0])
	dst, _ := os.Stat(dir)
	if st.Mode().Perm() != 0640 || dst.Mode().Perm() != 0750 {
		t.Fatalf("modes ticket=%v dir=%v", st.Mode(), dst.Mode())
	}
	b, _ := os.ReadFile(files[0])
	if rec, err := parseTicket(b); err != nil || rec.id != t1.id() || rec.n != 256*mib {
		t.Fatalf("record %q: %v", b, err)
	}
	rec := httptest.NewRecorder()
	g.serveTickets(rec)
	if !strings.Contains(rec.Body.String(), `"live_tickets":1`) || !strings.Contains(rec.Body.String(), `"inode":`) {
		t.Fatalf("metric: %s", rec.Body.String())
	}
	_, e = g.admit(256 * mib)
	if e == nil || e.status != 503 || e.code != "capacity_busy" || e.retryAfter == "" || !strings.Contains(e.message, "needs") {
		t.Fatalf("second: %+v", e)
	}
	t1.unref()
	g.waiters.Wait()
	if f := ticketFiles(t, dir); len(f) != 0 {
		t.Fatalf("ledger not empty after release: %v", f)
	}
	// Small requests never reach the ledger.
	if s := (&gateScope{g: g}); s.ensure(largeBodyBytes-1) != nil || s.t != nil {
		t.Fatal("a body under 64 MiB got a ticket")
	}
}

func TestGateRefusesWhenIdleHostCannotFit(t *testing.T) {
	_, rm := footprint(256 * mib)
	g := testGate(t, t.TempDir(), 4_660_000_000, rm+232*mib-1)
	if _, e := g.admit(256 * mib); e == nil || e.status != 503 || e.code != "insufficient_memory" {
		t.Fatalf("memory: %+v", e)
	}
	g = testGate(t, t.TempDir(), 3<<30, 901*mib)
	if _, e := g.admit(256 * mib); e == nil || e.status != 507 || e.code != "insufficient_storage" {
		t.Fatalf("disk: %+v", e)
	}
}

// An inner hop's LOCK_SH keeps the ticket reserved after keeper's request
// ends; it is released only when that lease is gone, with no timer.
func TestGateReleaseWaitsForInnerLease(t *testing.T) {
	dir := t.TempDir()
	g := testGate(t, dir, 1<<40, 1<<40)
	tk, e := g.admit(64 * mib)
	if e != nil {
		t.Fatal(e)
	}
	inner, err := os.OpenFile(g.ticketPath(tk.id()), os.O_RDONLY|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := unix.Flock(int(inner.Fd()), unix.LOCK_SH); err != nil {
		t.Fatal(err)
	}
	tk.unref()
	if _, e := g.admit(64 * mib); e != nil { // the held ticket still counts but more fit
		t.Fatal(e)
	}
	if n := len(ticketFiles(t, dir)); n != 2 {
		t.Fatalf("released while an inner lease was held: %d tickets", n)
	}
	inner.Close()
	// The second ticket's handler reference is never dropped here, so wait
	// only for the first one to go.
	deadline := time.Now().Add(5 * time.Second)
	for len(ticketFiles(t, dir)) != 1 {
		if time.Now().After(deadline) {
			t.Fatal("ticket not released after the inner lease closed")
		}
		time.Sleep(time.Millisecond)
	}
}

// A restarted keeper starts one waiter per ticket left behind and releases
// it once no lease remains; an unparsable ticket stops startup.
func TestGateRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	g := testGate(t, dir, 1<<40, 1<<40)
	tk, e := g.admit(64 * mib)
	if e != nil {
		t.Fatal(e)
	}
	inner, _ := os.OpenFile(g.ticketPath(tk.id()), os.O_RDONLY, 0)
	_ = unix.Flock(int(inner.Fd()), unix.LOCK_SH)
	tk.f.Close() // keeper dies: the kernel drops its own lease only
	g2 := testGate(t, dir, 1<<40, 1<<40)
	if n := len(ticketFiles(t, dir)); n != 1 {
		t.Fatalf("restart released a leased ticket: %d", n)
	}
	inner.Close()
	g2.waiters.Wait()
	if n := len(ticketFiles(t, dir)); n != 0 {
		t.Fatalf("restart did not release: %d", n)
	}
	bad := filepath.Join(dir, strings.Repeat("a", 32)+ticketSuffix)
	if err := os.WriteFile(bad, []byte("partial"), 0640); err != nil {
		t.Fatal(err)
	}
	if _, err := newCapacityGate(dir, "", 0, ""); err == nil || !strings.Contains(err.Error(), bad) {
		t.Fatalf("unparsable ticket accepted: %v", err)
	}
}

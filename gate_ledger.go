package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

type capacityGate struct {
	dir       string
	lock      *os.File // ledger.lock: LOCK_EX serializes only the ledger arithmetic
	storeFile string
	storeMax  int64
	memInfo   string
	// Test hooks; nil means the real source.
	memAvailable func() (int64, error)
	diskFree     func() (int64, error)
	waiters      sync.WaitGroup
}

// newCapacityGate opens the ledger and starts one release waiter per ticket
// left by a previous keeper. An unparsable ticket is a startup error.
func newCapacityGate(dir, storeFile string, storeMax int64, memInfo string) (*capacityGate, error) {
	if err := os.MkdirAll(dir, 0750); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0750); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, ledgerLockName), os.O_RDWR|os.O_CREATE|unix.O_CLOEXEC, 0640)
	if err != nil {
		return nil, err
	}
	g := &capacityGate{dir: dir, lock: lock, storeFile: storeFile, storeMax: storeMax, memInfo: memInfo}
	recs, files, err := g.liveTickets(true)
	if err != nil {
		lock.Close()
		return nil, err
	}
	for i, rec := range recs {
		log.Printf("capacity ledger: ticket %s (N=%d) left by a previous keeper; releasing once no lease remains", rec.id, rec.n)
		g.wait(rec, files[i])
	}
	return g, nil
}

func flockRetry(fd uintptr, how int) error {
	for {
		if err := unix.Flock(int(fd), how); !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func (g *capacityGate) lockLedger() error { return flockRetry(g.lock.Fd(), unix.LOCK_EX) }
func (g *capacityGate) unlockLedger()     { _ = flockRetry(g.lock.Fd(), unix.LOCK_UN) }

func (g *capacityGate) ticketPath(id string) string { return filepath.Join(g.dir, id+ticketSuffix) }

// liveTickets parses every ticket; with open it also returns an O_RDONLY fd
// per ticket. Callers hold ledger.lock, except at startup.
func (g *capacityGate) liveTickets(open bool) ([]ticketRec, []*os.File, error) {
	entries, err := os.ReadDir(g.dir)
	if err != nil {
		return nil, nil, err
	}
	var recs []ticketRec
	var files []*os.File
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ticketSuffix) {
			continue
		}
		path := filepath.Join(g.dir, e.Name())
		f, err := os.OpenFile(path, os.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, nil, err
		}
		b := make([]byte, ticketLen+1)
		n, _ := f.ReadAt(b, 0)
		rec, err := parseTicket(b[:n])
		if err == nil && rec.id+ticketSuffix != e.Name() {
			err = errors.New("ticket id does not match its file name")
		}
		if err != nil || !open {
			f.Close()
		}
		if err != nil {
			for _, o := range files {
				o.Close()
			}
			return nil, nil, fmt.Errorf("capacity ledger: unparsable ticket %s: %v (see the runbook)", path, err)
		}
		recs = append(recs, rec)
		if open {
			files = append(files, f)
		}
	}
	return recs, files, nil
}

// available returns disk and memory headroom above the floors, before live
// tickets: statfs free - 1 GiB - store headroom; MemAvailable - 232 MiB.
func (g *capacityGate) available() (disk, mem int64, err error) {
	if g.diskFree != nil {
		disk, err = g.diskFree()
	} else {
		var st unix.Statfs_t
		err = unix.Statfs(g.dir, &st)
		disk = int64(st.Bavail) * st.Frsize
	}
	if err != nil {
		return 0, 0, err
	}
	headroom := g.storeMax
	var st unix.Stat_t
	if unix.Stat(g.storeFile, &st) == nil {
		headroom = max(0, g.storeMax-st.Blocks*512) // on-disk size, not live payload (review n5)
	}
	if g.memAvailable != nil {
		mem, err = g.memAvailable()
	} else {
		mem, err = readMemAvailable(g.memInfo)
	}
	return disk - spoolDiskFloor - headroom, mem - memFloor - memMargin, err
}

func ledgerUnavailable(err error) *apiError {
	return &apiError{status: http.StatusServiceUnavailable, typ: "server_error", code: "capacity_ledger_unavailable",
		message: "keeper: the capacity ledger is unavailable: " + boundedText(err.Error())}
}

// refusal names required and available bytes and the floors (D4).
func refusal(rd, rm, dA, mA, liveD, liveM int64) *apiError {
	floors := fmt.Sprintf("floors: %d-byte disk reserve plus store headroom, %d-byte memory floor plus %d-byte margin", spoolDiskFloor, memFloor, memMargin)
	switch {
	case rd <= dA && rm <= mA:
		return &apiError{status: http.StatusServiceUnavailable, typ: "server_error", code: "capacity_busy", retryAfter: retryAfterHint,
			message: fmt.Sprintf("keeper: capacity busy: the request needs %d disk and %d memory bytes; %d disk and %d memory bytes are free after live tickets (%s)", rd, rm, dA-liveD, mA-liveM, floors)}
	case rd > dA:
		return &apiError{status: http.StatusInsufficientStorage, typ: "server_error", code: "insufficient_storage",
			message: fmt.Sprintf("keeper: insufficient storage: the request needs %d disk bytes; %d are available (%s)", rd, dA, floors)}
	}
	return &apiError{status: http.StatusServiceUnavailable, typ: "server_error", code: "insufficient_memory",
		message: fmt.Sprintf("keeper: insufficient memory: the request needs %d memory bytes; %d are available (%s)", rm, mA, floors)}
}

// fits checks the ledger with self's current reservation excluded.
func (g *capacityGate) fits(rd, rm int64, self *ticketRec) *apiError {
	recs, _, err := g.liveTickets(false)
	if err != nil {
		return ledgerUnavailable(err)
	}
	dA, mA, err := g.available()
	if err != nil {
		return ledgerUnavailable(err)
	}
	var liveD, liveM int64
	for _, r := range recs {
		if self == nil || r.id != self.id {
			liveD, liveM = liveD+r.rDisk, liveM+r.rMem
		}
	}
	if rd <= dA-liveD && rm <= mA-liveM {
		return nil
	}
	return refusal(rd, rm, dA, mA, liveD, liveM)
}

// serveTickets reports the live tickets: count, and per ticket its age,
// sizes and inode (no holder PIDs, review m1). Read-only; no expiry.
func (g *capacityGate) serveTickets(w http.ResponseWriter) {
	if err := g.lockLedger(); err != nil {
		writeAPIError(w, ledgerUnavailable(err))
		return
	}
	recs, _, err := g.liveTickets(false)
	type row struct {
		ID         string  `json:"id"`
		AgeSeconds float64 `json:"age_seconds"`
		N          int64   `json:"n"`
		RDisk      int64   `json:"r_disk"`
		RMem       int64   `json:"r_mem"`
		Inode      uint64  `json:"inode"`
	}
	rows := []row{}
	for _, r := range recs {
		var st unix.Stat_t
		if unix.Stat(g.ticketPath(r.id), &st) != nil {
			continue
		}
		age := time.Since(time.Unix(st.Ctim.Unix())).Seconds()
		rows = append(rows, row{r.id, age, r.n, r.rDisk, r.rMem, st.Ino})
	}
	g.unlockLedger()
	if err != nil {
		writeAPIError(w, ledgerUnavailable(err))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"live_tickets": len(rows), "tickets": rows})
}

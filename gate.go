package main

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// Host-wide admission gate (design D4). Only keeper writes the ledger; the
// inner hops mount it read-only and lease tickets with flock(LOCK_SH).
const (
	ticketSuffix   = ".ticket"
	ledgerLockName = "ledger.lock"
	ticketFormat   = "ticket-v1 %s %020d %020d %020d\n" // id, N, R_disk, R_mem
	ticketLen      = 10 + 32 + 3*21 + 1                 // "ticket-v1 " + id + 3 x " %020d" + "\n"
	memFloor       = 200 << 20
	memMargin      = 32 << 20
	skelTicketed   = 8 << 20  // S_ticket (D2)
	pageCacheBound = 16 << 20 // P_h (D3)
	kDiskTotal     = 7        // keeper 2 + bridge 1 + Sub2API 1 + AISIX 1 + CPA 2 (D4)
	retryAfterHint = "30"     // fixed hint, not a timer
)

// hopMem is one hop's c_h = k_mem*N + k_skel*S_ticket + P_h, or a flat term.
type hopMem struct {
	name    string
	flat    int64
	kSkel   int64
	kMem    bool // k_mem = 0.1 (acceptance threshold for patched hops)
	overlap bool // a Sub2API failover can overlap its teardown: doubled
}

var memHops = []hopMem{
	{name: "keeper", flat: 16 << 20},
	{name: "bridge", flat: 16 << 20},
	{name: "sub2api", kSkel: 17, kMem: true},
	{name: "aisix", kSkel: 5, kMem: true, overlap: true},
	{name: "cpa", kSkel: 9, kMem: true, overlap: true},
}

// footprint returns R_disk and R_mem for a ticketed body of n bytes.
func footprint(n int64) (rDisk, rMem int64) {
	for _, h := range memHops {
		c := h.flat
		if h.flat == 0 {
			c = h.kSkel*skelTicketed + pageCacheBound
			if h.kMem {
				c += (n + 9) / 10
			}
		}
		if h.overlap {
			c *= 2
		}
		rMem += c
	}
	return kDiskTotal * n, rMem
}

type ticketRec struct {
	id             string
	n, rDisk, rMem int64
}

func (t ticketRec) bytes() []byte {
	return []byte(fmt.Sprintf(ticketFormat, t.id, t.n, t.rDisk, t.rMem))
}

func parseTicket(b []byte) (ticketRec, error) {
	f := strings.Fields(string(b))
	if len(b) != ticketLen || len(f) != 5 || f[0] != "ticket-v1" || len(f[1]) != 32 {
		return ticketRec{}, errors.New("malformed ticket")
	}
	if _, err := hex.DecodeString(f[1]); err != nil {
		return ticketRec{}, err
	}
	var v [3]int64
	for i := range v {
		x, err := strconv.ParseInt(f[i+2], 10, 64)
		if err != nil || x < 0 {
			return ticketRec{}, errors.New("malformed ticket size")
		}
		v[i] = x
	}
	return ticketRec{f[1], v[0], v[1], v[2]}, nil
}

func newTicketID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}

// readMemAvailable reads MemAvailable (bytes) from a meminfo file.
func readMemAvailable(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if rest, ok := strings.CutPrefix(sc.Text(), "MemAvailable:"); ok {
			kb, err := strconv.ParseInt(strings.TrimSuffix(strings.TrimSpace(rest), " kB"), 10, 64)
			if err != nil {
				return 0, err
			}
			return kb << 10, nil
		}
	}
	return 0, errors.New("MemAvailable missing from " + path)
}

// createTicketFile links a fully written, fsynced ticket into dir under
// name: O_TMPFILE + linkat(/proc/self/fd/N, AT_SYMLINK_FOLLOW), which needs no
// capability (review m2). The returned fd is O_RDWR for in-place resizing.
func createTicketFile(dir, name string, content []byte) (*os.File, error) {
	fd, err := unix.Open(dir, unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0640)
	if err != nil {
		return nil, fmt.Errorf("ledger O_TMPFILE: %w", err)
	}
	f := os.NewFile(uintptr(fd), filepath.Join(dir, name))
	fail := func(err error) (*os.File, error) { f.Close(); return nil, err }
	if err := unix.Fchmod(fd, 0640); err != nil {
		return fail(err)
	}
	if _, err := f.Write(content); err != nil {
		return fail(err)
	}
	if err := f.Sync(); err != nil {
		return fail(err)
	}
	if err := unix.Linkat(unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", fd), unix.AT_FDCWD, filepath.Join(dir, name), unix.AT_SYMLINK_FOLLOW); err != nil {
		return fail(fmt.Errorf("ledger linkat: %w", err))
	}
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		d.Close()
	}
	return f, nil
}

package main

import (
	"context"
	"log"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const ticketHeader = "X-AI-Gateway-Capacity-Ticket"
const ticketMember = "ai_gateway_capacity_ticket"

// ticketPrefixLen is len(`{"ai_gateway_capacity_ticket":"<32 hex>",`).
const ticketPrefixLen = len(`{"`+ticketMember+`":"`) + 32 + len(`",`)

// capTicket is keeper's reservation plus its own lease (LOCK_SH on f). refs
// counts the handler and every spool holding the request's bytes; only at
// zero is the lease dropped and the release waiter started.
type capTicket struct {
	g    *capacityGate
	f    *os.File // O_RDWR, LOCK_SH
	mu   sync.Mutex
	rec  ticketRec
	refs int
}

// admit reserves the footprint of an n-byte body or refuses at once.
func (g *capacityGate) admit(n int64) (*capTicket, *apiError) {
	rd, rm := footprint(n)
	id, err := newTicketID()
	if err != nil {
		return nil, ledgerUnavailable(err)
	}
	if err := g.lockLedger(); err != nil {
		return nil, ledgerUnavailable(err)
	}
	defer g.unlockLedger()
	if e := g.fits(rd, rm, nil); e != nil {
		return nil, e
	}
	rec := ticketRec{id: id, n: n, rDisk: rd, rMem: rm}
	f, err := createTicketFile(g.dir, id+ticketSuffix, rec.bytes())
	if err != nil {
		return nil, ledgerUnavailable(err)
	}
	if err := flockRetry(f.Fd(), unix.LOCK_SH); err != nil {
		_ = os.Remove(g.ticketPath(id))
		f.Close()
		return nil, ledgerUnavailable(err)
	}
	return &capTicket{g: g, f: f, rec: rec, refs: 1}, nil
}

// resize re-reserves for n bytes (raising is re-admitted; lowering is always
// safe). The fixed-length record is rewritten in place under ledger.lock.
func (t *capTicket) resize(n int64) *apiError {
	t.mu.Lock()
	defer t.mu.Unlock()
	if n == t.rec.n {
		return nil
	}
	rd, rm := footprint(n)
	if err := t.g.lockLedger(); err != nil {
		return ledgerUnavailable(err)
	}
	defer t.g.unlockLedger()
	if n > t.rec.n {
		if e := t.g.fits(rd, rm, &t.rec); e != nil {
			return e
		}
	}
	rec := ticketRec{id: t.rec.id, n: n, rDisk: rd, rMem: rm}
	if _, err := t.f.WriteAt(rec.bytes(), 0); err != nil {
		return ledgerUnavailable(err)
	}
	if err := t.f.Sync(); err != nil {
		return ledgerUnavailable(err)
	}
	t.rec = rec
	return nil
}

func (t *capTicket) id() string { return t.rec.id }

// ref adds a holder; a ticket already released cannot be re-held.
func (t *capTicket) ref() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.refs == 0 {
		return false
	}
	t.refs++
	return true
}

// unref drops one holder; the last one ends the large request: it starts
// the release waiter and asks for the heap release.
func (t *capTicket) unref() {
	t.mu.Lock()
	t.refs--
	last := t.refs == 0
	rec := t.rec
	t.mu.Unlock()
	if last {
		t.g.wait(rec, t.f)
		t.g.heap.request()
	}
}

// wait releases a ticket once every lease is gone: a blocking LOCK_EX on the
// ticket (converting keeper's own LOCK_SH), then unlink under ledger.lock.
// It waits on the kernel lock only; there is no timer or expiry.
func (g *capacityGate) wait(rec ticketRec, f *os.File) {
	g.waiters.Add(1)
	go func() {
		defer g.waiters.Done()
		defer f.Close()
		if err := flockRetry(f.Fd(), unix.LOCK_EX); err != nil {
			log.Printf("capacity ledger: ticket %s release wait failed, stays reserved: %v", rec.id, err)
			return
		}
		if err := g.lockLedger(); err != nil {
			log.Printf("capacity ledger: ticket %s release failed, stays reserved: %v", rec.id, err)
			return
		}
		defer g.unlockLedger()
		if err := os.Remove(g.ticketPath(rec.id)); err != nil {
			log.Printf("capacity ledger: ticket %s unlink failed, stays reserved: %v", rec.id, err)
		}
	}()
}

// gateScope carries one request's ticket between admission sites.
type gateScope struct {
	g       *capacityGate
	t       *capTicket
	dropped bool // the handler reference is gone; t stays for later attaches
}

type gateScopeKey struct{}

func withGateScope(ctx context.Context, g *capacityGate) (context.Context, *gateScope) {
	s := &gateScope{g: g}
	return context.WithValue(ctx, gateScopeKey{}, s), s
}

func gateScopeFrom(ctx context.Context) *gateScope {
	s, _ := ctx.Value(gateScopeKey{}).(*gateScope)
	return s
}

// ensure admits an n-byte request once it reaches the ticket threshold.
func (s *gateScope) ensure(n int64) *apiError {
	if s == nil || s.g == nil || s.t != nil || n < largeBodyBytes {
		return nil
	}
	t, e := s.g.admit(n)
	if e != nil {
		return e
	}
	s.t = t
	return nil
}

// done drops the scope's handler reference. Work that outlives the handler
// (a background task) may still attach while a spool holds the ticket.
func (s *gateScope) done() {
	if s != nil && s.t != nil && !s.dropped {
		s.dropped = true
		s.t.unref()
	}
}

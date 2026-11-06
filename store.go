package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

// Mutations serialize independently of short generation selection and reads.
// Each node adds ONE item to a content-addressed prefix; full-history fallback
// therefore shares prefixes too. No response stores a growing hash manifest.
type store struct {
	mu                                  storeMutation
	generationMu                        sync.Mutex
	current                             *storeGeneration
	generations                         []*storeGeneration
	closed                              bool
	closing                             chan struct{}
	retiredErr                          error
	committed                           atomic.Value
	db                                  *bolt.DB
	lock                                *os.File
	dir, path                           string
	ttl                                 time.Duration
	maxLive, maxMeta, pinMax, compactAt int64
	maxEntries                          int64
	now                                 func() time.Time
	compactCopy                         func(*bolt.DB, *bolt.DB, int64) error
	stageCommitted                      func() error
	inputWork                           *workingSet
	staging                             *preparedStoreSource // protected by mu; owners pay for their plans
}

const (
	nodeMetaOverhead int64 = 160 // key + parent/hash/owner/ref record + overhead
	hashMetaOverhead int64 = 96  // item key + refcount + overhead
	respMetaOverhead int64 = 128 // plus variable response id bytes
	pinMetaOverhead  int64 = 128
)

var (
	bucketItems     = []byte("items")
	bucketItemRefs  = []byte("itemrefs")
	bucketNodes     = []byte("nodes")
	bucketResponses = []byte("responses")
	bucketPins      = []byte("pins")
	bucketCounts    = []byte("counts")
	errMissingItem  = errors.New("history item missing on disk")
	errStoreBudget  = errors.New("history exceeds store budget")
	errStoreOwner   = errors.New("response id already stored for another owner")
)

// itemSource streams one JSON history item per call; the returned slice is
// valid only until the next call. It must return io.EOF when drained. A
// source is single-pass: each consumer needs a fresh source from a factory.
type itemSource func(buf []byte) ([]byte, error)
type itemFactory func() itemSource

// sliceItems adapts an in-memory slice to the streaming interface factory.
func sliceItems(full []json.RawMessage) itemFactory {
	return func() itemSource {
		i := 0
		return func([]byte) ([]byte, error) {
			if i >= len(full) {
				return nil, io.EOF
			}
			item := full[i]
			i++
			return item, nil
		}
	}
}

func boltOptions() *bolt.Options {
	return &bolt.Options{NoSync: true, NoGrowSync: true, NoFreelistSync: true, FreelistType: bolt.FreelistMapType, Timeout: time.Second}
}

// A large initial mmap keeps the mapping stable while staged item payloads
// grow the file; each growth would otherwise force bbolt to dereference the
// whole dirty write set into heap (O(staged bytes) per remap).
const initialMmapSize = 128 << 20

func newStore(dir string, ttl time.Duration, maxLive, maxMeta, pinMax, compactAt int64) (*store, error) {
	if dir == "" || maxLive < 0 {
		return nil, errors.New("store dir and non-negative maxLive required")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// Never unlink this lock file: ownership must survive replacement of keeper.db.
	lock, err := os.OpenFile(filepath.Join(dir, "keeper.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("store already owned: %w", err)
	}
	path := filepath.Join(dir, "keeper.db")
	opts := boltOptions()
	opts.InitialMmapSize = initialMmapSize
	db, err := bolt.Open(path, 0600, opts)
	if err != nil {
		lock.Close()
		return nil, err
	} // corrupt cache: fail safely, never unlink on failed Open
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{bucketItems, bucketItemChunks, bucketItemRefs, bucketNodes, bucketResponses, bucketPins, bucketCounts} {
			if tx.Bucket(name) != nil {
				if err := tx.DeleteBucket(name); err != nil {
					return err
				}
			}
			if _, err := tx.CreateBucket(name); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		db.Close()
		lock.Close()
		return nil, err
	}
	if maxMeta <= 0 {
		maxMeta = 32 << 20
	}
	if maxLive > 0 && (pinMax <= 0 || pinMax > maxLive) {
		pinMax = maxLive / 2
	} else if pinMax <= 0 {
		pinMax = defaultCapBytes
	}
	if compactAt <= 0 {
		compactAt = 400 << 20
		if maxLive > 0 {
			compactAt = maxLive * 2
		}
	}
	g := &storeGeneration{db: db, done: make(chan struct{})}
	s := &store{db: db, current: g, generations: []*storeGeneration{g}, lock: lock, dir: dir, path: path, ttl: ttl, maxLive: maxLive, maxMeta: maxMeta, pinMax: pinMax, compactAt: compactAt, now: time.Now, compactCopy: bolt.Compact, inputWork: newWorkingSet(), closing: make(chan struct{})}
	s.committed.Store(storeStats{})
	return s, nil
}

func fileSizeOf(path string) int64 {
	if fi, err := os.Stat(path); err == nil {
		return fi.Size()
	}
	return 0
}
func number(v []byte) int64 {
	if len(v) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(v))
}
func encoded(n int64) []byte {
	v := make([]byte, 8)
	binary.BigEndian.PutUint64(v, uint64(n))
	return v
}

// These counters exist only inside a write transaction. Rollback also rolls
// back every reference, deletion, alias, pin and counter.
type storeTx struct {
	store              *store
	tx                 *bolt.Tx
	live, meta, pinned int64
}

func (s *store) update(fn func(*storeTx) error) error {
	s.generationMu.Lock()
	closed := s.closed
	s.generationMu.Unlock()
	if closed {
		return errStoreClosed
	}
	var snapshot storeStats
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCounts)
		st := &storeTx{store: s, tx: tx, live: number(b.Get([]byte("live"))), meta: number(b.Get([]byte("meta"))), pinned: number(b.Get([]byte("pinned")))}
		if err := fn(st); err != nil {
			return err
		}
		for k, v := range map[string]int64{"live": st.live, "meta": st.meta, "pinned": st.pinned} {
			if err := b.Put([]byte(k), encoded(v)); err != nil {
				return err
			}
		}
		// Bucket.Stats reports pre-spill KeyN in a writable transaction.
		if err := tx.Bucket(bucketResponses).ForEach(func(_, _ []byte) error {
			snapshot.entries++
			return nil
		}); err != nil {
			return err
		}
		snapshot.live = st.live
		return nil
	})
	if err == nil {
		s.committed.Store(snapshot)
	}
	return err
}

// Node: parent[32], item hash[32], owner[32], references[8]. A zero item
// hash denotes the empty-history root. Every other node has exactly one item.
// Nodes are stored only as pending or referenced; refs==0 nodes are deleted
// when their tx rolls back, or by maintenance if a crash left them.
func (st *storeTx) ref(id []byte, delta int64) error {
	b := st.tx.Bucket(bucketNodes)
	for len(id) > 0 {
		raw := b.Get(id)
		if len(raw) != 104 {
			return errMissingItem
		}
		v := append([]byte(nil), raw...)
		refs := number(v[96:]) + delta
		if refs > 0 {
			copy(v[96:], encoded(refs))
			return b.Put(id, v)
		}
		if err := b.Delete(id); err != nil {
			return err
		}
		st.meta -= nodeMetaOverhead
		var zero [32]byte
		if string(v[32:64]) != string(zero[:]) {
			h := v[32:64]
			refs := st.tx.Bucket(bucketItemRefs)
			r := refs.Get(h)
			if len(r) != 16 {
				return errMissingItem
			}
			if number(r[8:]) == 1 {
				st.live -= number(r[:8])
				st.meta -= hashMetaOverhead
				if st.store.stagedItem(h) {
					next := append([]byte(nil), r...)
					copy(next[8:], encoded(0))
					if err := refs.Put(h, next); err != nil {
						return err
					}
				} else {
					if err := refs.Delete(h); err != nil {
						return err
					}
					if err := deleteStoredItem(st.tx, h); err != nil {
						return err
					}
				}
			} else {
				next := append([]byte(nil), r...)
				copy(next[8:], encoded(number(r[8:])-1))
				if err := refs.Put(h, next); err != nil {
					return err
				}
			}
		}
		if string(v[:32]) == string(zero[:]) {
			return nil
		}
		id = v[:32]
		delta = -1
	}
	return nil
}

// planNode is one node the final link transaction must create.
type planNode struct {
	id, parent, hash [32]byte
}

// chainPlan is the result of one streaming pass over the history: the tip
// node to link, the new nodes to create, and the unique payload charge for
// pin accounting. Item payloads are staged separately by writeNewItems.
type chainPlan struct {
	tip      [32]byte
	charge   int64
	newNodes []planNode
	newItems map[[32]byte]bool
}

// planChain scans the history once with a single item in heap at a time.
// Node ids are deterministic (parent|item hash|owner), so the plan is exact.
func (s *store) planChain(owner [32]byte, next itemSource) (*chainPlan, error) {
	plan := &chainPlan{newItems: map[[32]byte]bool{}}
	seen := map[[32]byte]bool{}
	var parent [32]byte
	buf := make([]byte, 0, 64<<10)
	err := s.db.View(func(tx *bolt.Tx) error {
		nodes, refs := tx.Bucket(bucketNodes), tx.Bucket(bucketItemRefs)
		for i := -1; ; i++ {
			var hash [32]byte
			if i >= 0 {
				item, err := next(buf[:0])
				if err == io.EOF {
					break
				}
				if err != nil {
					return err
				}
				if !json.Valid(item) {
					return errors.New("invalid history JSON")
				}
				hash = sha256.Sum256(item)
				if !seen[hash] {
					seen[hash] = true
					plan.charge += int64(len(item))
				}
				if refs.Get(hash[:]) == nil {
					plan.newItems[hash] = true
				}
			}
			v := make([]byte, 96)
			copy(v, parent[:])
			copy(v[32:], hash[:])
			copy(v[64:], owner[:])
			id := sha256.Sum256(v)
			if nodes.Get(id[:]) == nil {
				plan.newNodes = append(plan.newNodes, planNode{id: id, parent: parent, hash: hash})
			}
			parent = id
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	plan.tip = parent
	return plan, nil
}

// writeNewItems commits staged item payloads (refs==0) in bounded write
// transactions so one commit never dirties the entire history. Payloads are
// immutable raw JSON in `items`; mutable state lives in `itemRefs` as a
// {payloadLen:8, refs:8} record so ref/link/drop touches only small rows.
// Orphans left by a failed accept are reclaimed by sweepOrphans or a
// restart wipe.
func (s *store) writeNewItems(next itemSource, want map[[32]byte]bool) error {
	const batch = 4 << 20
	buf := make([]byte, 0, 64<<10)
	var data [][]byte
	var hashes [][32]byte
	var size int64
	commit := func() error {
		if len(data) == 0 {
			return nil
		}
		err := s.db.Update(func(tx *bolt.Tx) error {
			its, refs := tx.Bucket(bucketItems), tx.Bucket(bucketItemRefs)
			for i, d := range data {
				k := hashes[i]
				if refs.Get(k[:]) == nil {
					if err := its.Put(k[:], d); err != nil {
						return err
					}
					rec := append(encoded(int64(len(d))), encoded(0)...)
					if err := refs.Put(k[:], rec); err != nil {
						return err
					}
				}
			}
			return nil
		})
		data, hashes, size = nil, nil, 0
		return err
	}
	for {
		item, err := next(buf[:0])
		if err == io.EOF {
			return commit()
		}
		if err != nil {
			return err
		}
		hash := sha256.Sum256(item)
		if !want[hash] {
			continue
		}
		delete(want, hash)
		data = append(data, append([]byte(nil), item...))
		hashes = append(hashes, hash)
		size += int64(len(item)) + hashMetaOverhead
		if size >= batch {
			if err := commit(); err != nil {
				return err
			}
		}
	}
}

// linkPlan applies the plan inside accept's single small final transaction:
// create new nodes, bump staged item refs, charging live/meta only for items
// actually adopted from refs==0.
func (st *storeTx) linkPlan(owner [32]byte, plan *chainPlan, maxMeta int64) error {
	nodes, refs := st.tx.Bucket(bucketNodes), st.tx.Bucket(bucketItemRefs)
	var added int64
	for _, n := range plan.newNodes {
		if nodes.Get(n.id[:]) != nil {
			continue
		}
		added += nodeMetaOverhead + hashMetaOverhead
		if added > maxMeta {
			return errStoreBudget
		}
		v := make([]byte, 104)
		copy(v, n.parent[:])
		copy(v[32:], n.hash[:])
		copy(v[64:], owner[:])
		var zero [32]byte
		if n.hash != zero {
			r := refs.Get(n.hash[:])
			if len(r) != 16 {
				return errMissingItem
			}
			if number(r[8:]) == 0 {
				st.live += number(r[:8])
				st.meta += hashMetaOverhead
				added += hashMetaOverhead
			}
			next := append([]byte(nil), r...)
			copy(next[8:], encoded(number(r[8:])+1))
			if err := refs.Put(n.hash[:], next); err != nil {
				return err
			}
			if err := st.ref(n.parent[:], 1); err != nil {
				return err
			}
		}
		if err := nodes.Put(n.id[:], v); err != nil {
			return err
		}
		st.meta += nodeMetaOverhead
	}
	return nil
}

// sweepOrphans deletes staged items that were never linked (refs==0).
// It collects the victims first, then deletes each payload in bounded
// batches so one transaction never dirties the whole orphan set. Runs
// inside maintainLocked; a crash leaves only garbage a later sweep removes.
func (st *storeTx) sweepOrphans() error {
	refs := st.tx.Bucket(bucketItemRefs)
	var orphans [][]byte
	var bytes int64
	const batch = 4 << 20
	flush := func() error {
		for _, k := range orphans {
			if err := refs.Delete(k); err != nil {
				return err
			}
			if err := deleteStoredItem(st.tx, k); err != nil {
				return err
			}
		}
		orphans, bytes = nil, 0
		return nil
	}
	c := refs.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if len(v) == 16 && number(v[8:]) == 0 && !st.store.stagedItem(k) {
			orphans = append(orphans, append([]byte(nil), k...))
			bytes += number(v[:8])
			if bytes >= batch {
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
	return flush()
}

func (st *storeTx) dropResponse(id []byte) error {
	b := st.tx.Bucket(bucketResponses)
	v := b.Get(id)
	if v == nil {
		return nil
	}
	node := append([]byte(nil), v[:32]...)
	st.meta -= respMetaOverhead + int64(len(id))
	if err := b.Delete(id); err != nil {
		return err
	}
	return st.ref(node, -1)
}
func (st *storeTx) dropPin(pin string) error {
	b := st.tx.Bucket(bucketPins)
	v := b.Get([]byte(pin))
	if v == nil {
		return nil
	}
	node := append([]byte(nil), v[:32]...)
	st.pinned -= number(v[32:])
	st.meta -= pinMetaOverhead
	if err := b.Delete([]byte(pin)); err != nil {
		return err
	}
	return st.ref(node, -1)
}
func (st *storeTx) expire(now time.Time) error {
	c := st.tx.Bucket(bucketResponses).Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {
		if number(v[32:40]) <= now.UnixNano() {
			if err := st.dropResponse(k); err != nil {
				return err
			}
		}
	}
	return nil
}

// Maintenance precedes admission. A failed required compaction rejects the
// incoming turn before changing its aliases/pins. Expired IDs are not promises.
func (s *store) maintainLocked() error {
	if err := s.update(func(st *storeTx) error { return st.expire(s.now()) }); err != nil {
		return err
	}
	if fileSizeOf(s.path) > s.compactAt {
		return s.compactLocked()
	}
	return nil
}

func (s *store) put(id string, owner [32]byte, _ string, full []json.RawMessage) (string, error) {
	node, _, err := s.accept(id, owner, sliceItems(full), "", true, false)
	return node, err
}

// accept atomically acquires the new history and optional socket pin, releases
// only that socket's old pin, and evicts aliases. Item payloads stream through
// `next` one at a time; staged payloads commit in bounded write transactions
// and the final link transaction touches only small node/alias/pin rows.
// Any failure rejects the turn: unlinked staged items are swept as orphans.
func (s *store) accept(id string, owner [32]byte, next itemFactory, oldPin string, registered, pinned bool) (string, string, error) {
	return s.acceptContext(context.Background(), id, owner, next, oldPin, registered, pinned)
}

func (s *store) acceptContext(ctx context.Context, id string, owner [32]byte, source itemFactory, oldPin string, registered, pinned bool) (string, string, error) {
	return s.acceptWithBudget(ctx, id, owner, source, oldPin, registered, pinned, nil)
}

func (s *store) acceptWithBudget(ctx context.Context, id string, owner [32]byte, source itemFactory, oldPin string, registered, pinned bool, reserve func() (int64, func())) (string, string, error) {
	return s.acceptStream(ctx, id, owner, streamItems(source), oldPin, registered, pinned, reserve)
}

func (s *store) acceptStream(ctx context.Context, id string, owner [32]byte, source itemStreamFactory, oldPin string, registered, pinned bool, reserve func() (int64, func())) (string, string, error) {
	prepared, err := s.prepareStoreSource(ctx, owner, source)
	if err != nil {
		return "", "", err
	}
	defer prepared.Close()
	if err := s.acquireMutation(ctx); err != nil {
		return "", "", err
	}
	if err := s.maintainLocked(); err != nil {
		s.mu.Unlock()
		return "", "", err
	}
	prepared.stageNext, s.staging = s.staging, prepared
	prepared.staging = true
	s.mu.Unlock()
	if err := s.writePreparedItems(ctx, prepared); err != nil {
		return "", "", err
	}
	if err := s.acquireMutation(ctx); err != nil {
		return "", "", err
	}
	defer s.mu.Unlock()
	var sharedLimit int64 = -1
	if reserve != nil {
		var release func()
		sharedLimit, release = reserve()
		defer release()
	}
	plan := prepared.plan
	var node, pin string
	err = s.update(func(st *storeTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		responses := st.tx.Bucket(bucketResponses)
		entries := int64(responses.Stats().KeyN)
		if registered {
			if old := responses.Get([]byte(id)); old != nil {
				n := st.tx.Bucket(bucketNodes).Get(old[:32])
				if string(n[64:96]) != string(owner[:]) {
					return errStoreOwner
				}
			} else {
				entries++
			}
		}
		if err := st.linkPlan(owner, plan, s.maxMeta); err != nil {
			return err
		}
		node = string(plan.tip[:])
		n := plan.tip[:]
		charge := plan.charge
		// Acquire before dropping/evicting dependencies, including identical aliases.
		if registered {
			if err := st.ref(n, 1); err != nil {
				return err
			}
			if err := st.dropResponse([]byte(id)); err != nil {
				return err
			}
			v := append(append(append([]byte(nil), n...), encoded(s.now().Add(s.ttl).UnixNano())...), encoded(s.now().UnixNano())...)
			if err := responses.Put([]byte(id), v); err != nil {
				return err
			}
			st.meta += respMetaOverhead + int64(len(id))
		}
		if pinned {
			if err := st.ref(n, 1); err != nil {
				return err
			}
			pins := st.tx.Bucket(bucketPins)
			seq, err := pins.NextSequence()
			if err != nil {
				return err
			}
			pin = string(encoded(int64(seq)))
			if err := pins.Put([]byte(pin), append(append([]byte(nil), n...), encoded(charge)...)); err != nil {
				return err
			}
			st.meta += pinMetaOverhead
			st.pinned += charge
			if err := st.dropPin(oldPin); err != nil {
				return err
			}
		}
		if st.pinned > s.pinMax {
			return errStoreBudget
		}
		for (s.maxLive > 0 && st.live > s.maxLive) || (sharedLimit >= 0 && st.live > sharedLimit) || st.meta > s.maxMeta || (s.maxEntries > 0 && entries > s.maxEntries) {
			var victim []byte
			var oldest int64
			if err := responses.ForEach(func(k, v []byte) error {
				if registered && string(k) == id {
					return nil
				}
				if victim == nil || number(v[40:]) < oldest {
					victim = append([]byte(nil), k...)
					oldest = number(v[40:])
				}
				return nil
			}); err != nil {
				return err
			}
			if victim == nil {
				return errStoreBudget
			}
			if err := st.dropResponse(victim); err != nil {
				return err
			}
			entries--
		}
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return node, pin, nil
}

func (s *store) putPinned(owner [32]byte, _, _ string, full []json.RawMessage) (string, func(), error) {
	_, pin, err := s.accept("", owner, sliceItems(full), "", false, true)
	return pin, func() { _ = s.release(pin) }, err
}
func (s *store) release(pin string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.update(func(st *storeTx) error { return st.dropPin(pin) })
}

// Compact in bounded library transactions. Rename while BOTH databases remain
// open; a failed copy/rename leaves the original pointer and path untouched.
// The stable directory lock remains held throughout, including close and swap.
func (s *store) compactLocked() error {
	tmp := s.path + ".compact"
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return err
	}
	dst, err := bolt.Open(tmp, 0600, boltOptions())
	if err != nil {
		return err
	}
	if err = s.compactCopy(dst, s.db, 1<<20); err != nil {
		dst.Close()
		os.Remove(tmp)
		return err
	}
	if err = os.Rename(tmp, s.path); err != nil {
		dst.Close()
		os.Remove(tmp)
		return err
	}
	s.publishGeneration(dst)
	return nil
}
func (s *store) stats() (entries int, bytes int64) {
	snapshot := s.committed.Load().(storeStats)
	return snapshot.entries, snapshot.live
}
func (s *store) diskUsage() int64 { s.mu.Lock(); defer s.mu.Unlock(); return fileSizeOf(s.path) }

// ownedID reports whether id is a local checkpoint alias for this owner. It
// distinguishes native upstream references from keeper-owned checkpoint ids
// without a private path or protocol flag. A miss on a known local id (wrong
// owner or expired) must fail closed, so callers can refuse to forward it.
func (s *store) ownedID(id string, owner [32]byte) (known bool, valid bool) {
	_ = s.view(func(tx *bolt.Tx) error {
		entry := tx.Bucket(bucketResponses).Get([]byte(id))
		if entry == nil {
			return nil
		}
		known = true
		if number(entry[32:40]) <= s.now().UnixNano() {
			return nil // expired alias
		}
		// Ownership lives on the chain's tip node record, same layout as
		// historyHashes: tip bytes 64:96 are the owning identity.
		node := tx.Bucket(bucketNodes).Get(entry[:32])
		valid = node != nil && len(node) == 104 && string(node[64:96]) == string(owner[:])
		return nil
	})
	return known, valid
}

func (s *store) forget(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.update(func(st *storeTx) error { return st.dropResponse([]byte(id)) })
}

func (s *store) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generationMu.Lock()
	if s.closed {
		s.generationMu.Unlock()
		return nil
	}
	s.closed = true
	close(s.closing)
	current := s.current
	current.retired = true
	closeNow := current.refs == 0
	generations := append([]*storeGeneration(nil), s.generations...)
	s.generationMu.Unlock()
	if closeNow {
		current.err = current.db.Close()
		close(current.done)
	}
	var err = s.retiredErr
	for _, generation := range generations {
		<-generation.done
		if err == nil {
			err = generation.err
		}
	}
	if s.lock != nil {
		e := s.lock.Close()
		s.lock = nil
		if err == nil {
			err = e
		}
	}
	return err
}

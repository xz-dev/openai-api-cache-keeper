package main

import (
	"context"
	"errors"
	"io"
	"sync"

	bolt "go.etcd.io/bbolt"
)

var errStoreClosed = errors.New("store closed")

// Only mutations serialize here; request processing and generation reads do not.
type storeMutation struct {
	once  sync.Once
	token chan struct{}
}

func (m *storeMutation) acquire(ctx context.Context) error {
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	select {
	case m.token <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-m.token
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (m *storeMutation) tryLock() bool {
	m.once.Do(func() { m.token = make(chan struct{}, 1) })
	select {
	case m.token <- struct{}{}:
		return true
	default:
		return false
	}
}

func (m *storeMutation) Lock()   { _ = m.acquire(context.Background()) }
func (m *storeMutation) Unlock() { <-m.token }

type storeGeneration struct {
	db      *bolt.DB
	refs    int
	retired bool
	done    chan struct{}
	err     error
}

type storeStats struct {
	entries int
	live    int64
}

func (s *store) acquireGeneration() (*storeGeneration, error) {
	s.generationMu.Lock()
	defer s.generationMu.Unlock()
	if s.closed {
		return nil, errStoreClosed
	}
	g := s.current
	g.refs++
	return g, nil
}

func (s *store) releaseGeneration(g *storeGeneration) {
	s.generationMu.Lock()
	g.refs--
	closeNow := g.retired && g.refs == 0
	s.generationMu.Unlock()
	if closeNow {
		g.err = g.db.Close()
		close(g.done)
	}
}

// Caller owns the mutation serializer. New readers can use the new generation
// even while an old snapshot is copying to a slow destination.
func (s *store) publishGeneration(db *bolt.DB) {
	s.generationMu.Lock()
	old := s.current
	next := &storeGeneration{db: db, done: make(chan struct{})}
	s.current, s.db = next, db
	live := s.generations[:0]
	for _, generation := range s.generations {
		select {
		case <-generation.done:
			if s.retiredErr == nil {
				s.retiredErr = generation.err
			}
		default:
			live = append(live, generation)
		}
	}
	s.generations = append(live, next)
	old.retired = true
	closeNow := old.refs == 0
	s.generationMu.Unlock()
	if closeNow {
		old.err = old.db.Close()
		close(old.done)
	}
}

func (s *store) view(fn func(*bolt.Tx) error) error {
	g, err := s.acquireGeneration()
	if err != nil {
		return err
	}
	defer s.releaseGeneration(g)
	return g.db.View(fn)
}

// A temporary chain pin owns content across eviction and generation changes.
// Each payload read transaction ends before the caller writes to its spool.
type historySnapshot struct {
	store        *store
	gen          *storeGeneration
	pin          string
	hashes       [][32]byte
	once         sync.Once
	err          error
	releaseInput func()
}

func (s *store) snapshotID(ctx context.Context, id string, owner [32]byte, pinned bool) (*historySnapshot, error) {
	return s.snapshot(ctx, owner, func(tx *bolt.Tx) ([32]byte, error) {
		bucket := bucketResponses
		if pinned {
			bucket = bucketPins
		}
		key := []byte(id) // one key copy serves Get and Put
		entry := tx.Bucket(bucket).Get(key)
		var tip [32]byte
		if len(entry) < 40 || (!pinned && number(entry[32:40]) <= s.now().UnixNano()) {
			return tip, errMissingItem
		}
		copy(tip[:], entry[:32])
		if !pinned && tx.Writable() {
			if len(entry) < 48 {
				return tip, errMissingItem
			}
			touched := append([]byte(nil), entry...)
			copy(touched[40:], encoded(s.now().UnixNano()))
			if err := tx.Bucket(bucketResponses).Put(key, touched); err != nil {
				return tip, err
			}
		}
		return tip, nil
	})
}

func (s *store) snapshotTip(ctx context.Context, tip [32]byte, owner [32]byte) (*historySnapshot, error) {
	return s.snapshot(ctx, owner, func(*bolt.Tx) ([32]byte, error) { return tip, nil })
}

func (s *store) snapshot(ctx context.Context, owner [32]byte, lookup func(*bolt.Tx) ([32]byte, error)) (*historySnapshot, error) {
	if err := s.acquireMutation(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	g, err := s.acquireGeneration()
	if err != nil {
		return nil, err
	}
	snapshot := &historySnapshot{store: s, gen: g}
	if g.db.IsReadOnly() {
		err = g.db.View(func(tx *bolt.Tx) error {
			tip, err := lookup(tx)
			if err != nil {
				return err
			}
			snapshot.hashes, snapshot.releaseInput, err = s.snapshotHashes(tx, tip, owner)
			return err
		})
		if err != nil {
			s.releaseGeneration(g)
			return nil, err
		}
		return snapshot, nil // immutable generation needs no writable chain pin
	}
	err = s.update(func(st *storeTx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		tip, err := lookup(st.tx)
		if err != nil {
			return err
		}
		snapshot.hashes, snapshot.releaseInput, err = s.snapshotHashes(st.tx, tip, owner)
		if err != nil {
			return err
		}
		// This is a read lease over already-live bytes, not another promised
		// socket history. Charge its metadata; item refs preserve live bytes.
		if st.meta+pinMetaOverhead > s.maxMeta {
			return errStoreBudget
		}
		pins := st.tx.Bucket(bucketPins)
		seq, err := pins.NextSequence()
		if err != nil {
			return err
		}
		snapshot.pin = string(encoded(int64(seq)))
		if err := st.ref(tip[:], 1); err != nil {
			return err
		}
		if err := pins.Put([]byte(snapshot.pin), append(append([]byte(nil), tip[:]...), encoded(0)...)); err != nil {
			return err
		}
		st.meta += pinMetaOverhead
		return ctx.Err()
	})
	if err != nil {
		if snapshot.releaseInput != nil {
			snapshot.releaseInput()
		}
		s.releaseGeneration(g)
		return nil, err
	}
	return snapshot, nil
}

func (h *historySnapshot) Close() error {
	h.once.Do(func() {
		h.hashes = nil
		if h.releaseInput != nil {
			h.releaseInput()
		}
		h.store.releaseGeneration(h.gen)
		if h.pin == "" {
			return
		}
		h.err = h.store.release(h.pin)
		if errors.Is(h.err, errStoreClosed) {
			h.err = nil // store shutdown discards all cache ownership together
		}
	})
	return h.err
}

type snapshotItemReader struct {
	history *historySnapshot
	ctx     context.Context
	hash    [32]byte
	offset  int64
	size    int64
}

func (r *snapshotItemReader) Read(dst []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.offset == r.size {
		return 0, io.EOF
	}
	var n int
	err := r.history.gen.db.View(func(tx *bolt.Tx) error {
		var err error
		n, err = readStoredItem(tx, r.hash[:], r.offset, dst[:min(int64(len(dst)), r.size-r.offset)])
		return err
	})
	r.offset += int64(n)
	return n, err
}

// storedSize sums the stored lengths of the snapshot's items without
// reading their contents (the expansion pre-check, task 1.2).
func (h *historySnapshot) storedSize(ctx context.Context) (total, items int64, err error) {
	var zero [32]byte
	err = h.gen.db.View(func(tx *bolt.Tx) error {
		for _, hash := range h.hashes {
			if hash == zero {
				continue
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			size, err := storedItemSize(tx, hash[:])
			if err != nil {
				return err
			}
			total += size
			items++
		}
		return nil
	})
	return total, items, err
}

func (h *historySnapshot) each(ctx context.Context, fn func(int64, io.Reader) error) error {
	for _, hash := range h.hashes {
		if err := ctx.Err(); err != nil {
			return err
		}
		var zero [32]byte
		if hash == zero {
			continue
		}
		var size int64
		if err := h.gen.db.View(func(tx *bolt.Tx) error {
			var err error
			size, err = storedItemSize(tx, hash[:])
			return err
		}); err != nil {
			return err
		}
		reader := &snapshotItemReader{history: h, ctx: ctx, hash: hash, size: size}
		if err := fn(size, reader); err != nil {
			return err
		}
		if reader.offset != size {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

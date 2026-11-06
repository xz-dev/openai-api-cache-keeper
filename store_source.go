package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"

	bolt "go.etcd.io/bbolt"
)

type preparedStoreItem struct {
	offset int64
	size   int
	hash   [32]byte
}
type preparedStoreSource struct {
	file      *spool
	hold      *spoolBody // set when items are read in place from the caller's spool
	plan      *chainPlan
	items     []preparedStoreItem
	credits   []func()
	store     *store
	gen       *storeGeneration
	stageNext *preparedStoreSource
	staging   bool
}

func (p *preparedStoreSource) Close() {
	if p.hold != nil {
		_ = p.hold.Close()
	} else if p.file != nil {
		_ = p.file.Close()
	}
	if p.gen != nil {
		// close holds mu while joining generations: release before taking mu.
		p.store.releaseGeneration(p.gen)
		p.gen = nil
	}
	if p.staging {
		s := p.store
		s.mu.Lock()
		for link := &s.staging; *link != nil; link = &(*link).stageNext {
			if *link == p {
				*link = p.stageNext
				break
			}
		}
		p.staging = false
		s.mu.Unlock()
		// Only this owner's hashes need reclamation; other live stages stay safe.
		for start := 0; start < len(p.items); {
			end, size := start, 0
			for end < len(p.items) && size < 4<<20 {
				size += p.items[end].size
				end++
			}
			if err := s.acquireMutation(context.Background()); err != nil {
				break
			}
			err := s.update(func(st *storeTx) error {
				refs := st.tx.Bucket(bucketItemRefs)
				for _, item := range p.items[start:end] {
					r := refs.Get(item.hash[:])
					// No refs record: a chunked write that never completed.
					if (r != nil && (len(r) != 16 || number(r[8:]) != 0)) || s.stagedItem(item.hash[:]) {
						continue
					}
					if err := refs.Delete(item.hash[:]); err != nil {
						return err
					}
					if err := deleteStoredItem(st.tx, item.hash[:]); err != nil {
						return err
					}
				}
				return nil
			})
			s.mu.Unlock()
			if err != nil {
				break // failed cleanup leaves reclaimable orphans, never aliases
			}
			start = end
		}
	}
	for _, free := range p.credits {
		free()
	}
	p.credits = nil
}

// Prepared plans already own charged hash maps; no second persistent map is needed.
// Caller holds the mutation serializer, including through compaction publication.
func (s *store) stagedItem(key []byte) bool {
	var hash [32]byte
	copy(hash[:], key)
	for p := s.staging; p != nil; p = p.stageNext {
		if p.plan.newItems[hash] {
			return true
		}
	}
	return false
}

// Caller item reads, validation, hashing and spool I/O must not run under the
// DB mutation serializer or in a bbolt transaction.
func (s *store) prepareStoreSource(ctx context.Context, owner [32]byte, source itemStreamFactory) (_ *preparedStoreSource, err error) {
	g, acquireErr := s.acquireGeneration()
	if acquireErr != nil {
		return nil, acquireErr
	}
	stateCredit, e := s.inputWork.reserveInput(512)
	if e != nil {
		s.releaseGeneration(g)
		return nil, e
	}
	p := &preparedStoreSource{store: s, gen: g, credits: []func(){stateCredit}, plan: &chainPlan{newItems: make(map[[32]byte]bool)}}
	defer func() {
		if err != nil {
			p.Close()
		}
	}()
	k := &keeper{cfg: config{storeDir: s.dir}, work: s.inputWork}
	// One small-item buffer plus the streaming validator of a large item.
	window, err := s.inputWork.reserveInput(64<<10 + jsonValidateCredit)
	if err != nil {
		return nil, err
	}
	defer window()
	buf := make([]byte, 64<<10)
	next := source()
	var parent [32]byte
	seenSpans := make(map[[32]byte]preparedStoreItem)
	addNode := func(hash [32]byte) error {
		free, e := s.inputWork.reserveInput(1024)
		if e != nil {
			return e
		}
		p.credits = append(p.credits, free)
		var raw [96]byte
		copy(raw[:32], parent[:])
		copy(raw[32:64], hash[:])
		copy(raw[64:], owner[:])
		id := sha256.Sum256(raw[:])
		p.plan.newNodes = append(p.plan.newNodes, planNode{id: id, parent: parent, hash: hash})
		parent = id
		return nil
	}
	if err = addNode([32]byte{}); err != nil {
		return nil, err
	}
	for {
		if err = ctx.Err(); err != nil {
			return nil, err
		}
		n, r, e := next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if int64(len(p.items)+2)*nodeMetaOverhead > s.maxMeta {
			return nil, errStoreBudget
		}
		// An item already on a stable spool is staged from there in place:
		// no second full-size copy (keeper disk 2N, gate k_disk 2, D4).
		inPlace, ok := r.(*spoolItemReader)
		switch {
		case p.file == nil && ok:
			if p.hold, err = inPlace.f.requestBody(); err != nil {
				return nil, err
			}
			p.file = inPlace.f
		case p.file == nil:
			if p.file, err = k.newSpool(); err != nil {
				return nil, err
			}
		case ok != (p.hold != nil) || (ok && inPlace.f != p.file):
			return nil, errors.New("store source mixes spools")
		}
		if !ok && p.file.size+n > p.file.mem.spoolMax {
			return nil, errWorkSize
		}
		var hash [32]byte
		offset := p.file.size
		var sink io.Writer = p.file
		if ok {
			offset, sink = inPlace.off, io.Discard
		}
		if n <= int64(len(buf)) {
			item := buf[:n]
			if _, err = io.ReadFull(r, item); err != nil {
				return nil, err
			}
			if !json.Valid(item) {
				return nil, errUpstreamData
			}
			hash = sha256.Sum256(item)
			if _, seen := seenSpans[hash]; !seen {
				if _, err = sink.Write(item); err != nil {
					return nil, err
				}
			}
		} else {
			// A large item streams through the validator into the spool;
			// a duplicate's copy stays unused there (disk only, in budget).
			h := sha256.New()
			counted := &countingWriter{w: io.MultiWriter(h, sink)}
			if err = validJSONStream(ctx, io.TeeReader(io.LimitReader(r, n), counted)); err != nil {
				if errors.Is(err, errJSONSyntax) {
					err = errUpstreamData
				}
				return nil, err
			}
			if counted.n != n {
				return nil, io.ErrUnexpectedEOF
			}
			h.Sum(hash[:0])
		}
		if err = addNode(hash); err != nil {
			return nil, err
		}
		span, exists := seenSpans[hash]
		if !exists {
			p.plan.charge += n
			p.plan.newItems[hash] = true
			span = preparedStoreItem{offset: offset, size: int(n), hash: hash}
			seenSpans[hash] = span
		}
		p.items = append(p.items, span)
	}
	p.plan.tip = parent
	return p, nil
}

// spoolItemReader streams one item stored at payload offset off of f; the
// store stages it in place while f stays held.
type spoolItemReader struct {
	io.Reader
	f   *spool
	off int64
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// storePiece is one bounded Put: a whole small item, or one chunk of a
// large one (chunk >= 0).
type storePiece struct {
	hash        [32]byte
	off         int64
	n           int
	chunk       int64
	size        int64 // whole item size, recorded with the last piece
	last, small bool
}

// storeWriteCredit is the heap one staging write may use: the payload slab
// plus bbolt's commit copies (leaf node buffer and overflow page
// allocation; TestStoreLargeItemHeapBounded measures about 10 MiB per
// transaction including the slab).
const storeWriteCredit int64 = 3 * (storeWriteBatch + itemChunkBytes)

// Only actual immutable payload mutations remain here. Data is validated,
// hashed and owned before entering the writer; no caller source can park it.
// Each write transaction carries at most storeWriteBatch (+ one chunk) of
// payload read from the prepared spool into one charged slab.
func (s *store) writePreparedItems(ctx context.Context, p *preparedStoreSource) error {
	var staged []storePiece
	var size int
	seen := make(map[[32]byte]bool)
	var slab []byte
	commit := func() error {
		if len(staged) == 0 {
			return nil
		}
		if slab == nil {
			free, err := s.inputWork.reserveInput(storeWriteCredit)
			if err != nil {
				return err
			}
			p.credits = append(p.credits, free)
			slab = make([]byte, storeWriteBatch+itemChunkBytes)
		}
		data := make([][]byte, len(staged))
		used := 0
		for i, piece := range staged {
			data[i] = slab[used : used+piece.n]
			used += piece.n
			if _, err := p.file.ReadAt(data[i], piece.off); err != nil {
				return err
			}
		}
		if err := s.acquireMutation(ctx); err != nil {
			return err
		}
		// Select the current DB under mu: compaction copied protected stages.
		// bbolt keeps each Put's value slice until commit: slab is reused after.
		err := s.db.Update(func(tx *bolt.Tx) error {
			items, chunks, refs := tx.Bucket(bucketItems), tx.Bucket(bucketItemChunks), tx.Bucket(bucketItemRefs)
			for i, piece := range staged {
				if refs.Get(piece.hash[:]) != nil {
					continue
				}
				var err error
				if piece.small {
					err = items.Put(piece.hash[:], data[i])
				} else {
					err = chunks.Put(chunkKey(piece.hash[:], piece.chunk), data[i])
				}
				if err == nil && piece.last {
					err = refs.Put(piece.hash[:], append(encoded(piece.size), encoded(0)...))
				}
				if err != nil {
					return err
				}
			}
			return nil
		})
		hook := s.stageCommitted
		s.mu.Unlock()
		staged = nil
		size = 0
		if err == nil && hook != nil {
			err = hook()
		}
		return err
	}
	add := func(piece storePiece) error {
		staged = append(staged, piece)
		size += piece.n
		if size >= storeWriteBatch {
			return commit()
		}
		return nil
	}
	for _, item := range p.items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if seen[item.hash] {
			continue
		}
		seen[item.hash] = true
		whole := int64(item.size)
		if whole <= itemChunkBytes {
			if err := add(storePiece{hash: item.hash, off: item.offset, n: item.size, size: whole, last: true, small: true}); err != nil {
				return err
			}
			continue
		}
		for i := int64(0); i*itemChunkBytes < whole; i++ {
			n := min(itemChunkBytes, whole-i*itemChunkBytes)
			last := (i+1)*itemChunkBytes >= whole
			if err := add(storePiece{hash: item.hash, off: item.offset + i*itemChunkBytes, n: int(n), chunk: i, size: whole, last: last}); err != nil {
				return err
			}
		}
	}
	return commit()
}

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log"

	bolt "go.etcd.io/bbolt"
	"golang.org/x/sys/unix"
)

type preparedStoreItem struct {
	offset int64
	size   int
	hash   [32]byte
}
type preparedStoreSource struct {
	file           *spool
	plan           *chainPlan
	items          []preparedStoreItem
	mapped         []byte
	credits        []func()
	mappingRelease func()
	store          *store
	gen            *storeGeneration
	stageNext      *preparedStoreSource
	staging        bool
}

func (p *preparedStoreSource) Close() {
	if p.mapped != nil {
		if err := unix.Munmap(p.mapped); err != nil {
			log.Printf("prepared source unmap failed; mapping remains charged: %v", err)
		} else {
			p.mapped = nil
		}
	}
	if p.mapped == nil && p.mappingRelease != nil {
		p.mappingRelease()
		p.mappingRelease = nil
	}
	if p.file != nil {
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
				refs, items := st.tx.Bucket(bucketItemRefs), st.tx.Bucket(bucketItems)
				for _, item := range p.items[start:end] {
					r := refs.Get(item.hash[:])
					if len(r) != 16 || number(r[8:]) != 0 || s.stagedItem(item.hash[:]) {
						continue
					}
					if err := refs.Delete(item.hash[:]); err != nil {
						return err
					}
					if err := items.Delete(item.hash[:]); err != nil {
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
func (s *store) prepareStoreSource(ctx context.Context, owner [32]byte, source itemFactory) (_ *preparedStoreSource, err error) {
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
	p.file, err = k.newSpool()
	if err != nil {
		return nil, err
	}
	window, err := s.inputWork.reserveInput(64 << 10)
	if err != nil {
		return nil, err
	}
	defer window()
	buf := make([]byte, 0, 64<<10)
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
		item, e := next(buf[:0])
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if int64(len(p.items)+2)*nodeMetaOverhead > s.maxMeta {
			return nil, errStoreBudget
		}
		if !json.Valid(item) {
			return nil, errUpstreamData
		}
		hash := sha256.Sum256(item)
		if err = addNode(hash); err != nil {
			return nil, err
		}
		span, exists := seenSpans[hash]
		if !exists {
			if p.file.size+int64(len(item)) > maxSpoolBytes {
				return nil, errWorkSize
			}
			p.plan.charge += int64(len(item))
			p.plan.newItems[hash] = true
			span = preparedStoreItem{offset: p.file.size, size: len(item), hash: hash}
			if _, err = p.file.Write(item); err != nil {
				return nil, err
			}
			seenSpans[hash] = span
		}
		p.items = append(p.items, span)
	}
	p.plan.tip = parent
	free, e := s.inputWork.reserveInput(p.file.offset + p.file.size)
	if e != nil {
		return nil, e
	}
	p.mappingRelease = free
	p.mapped, err = unix.Mmap(int(p.file.File.Fd()), 0, int(p.file.offset+p.file.size), unix.PROT_READ, unix.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}
	return p, nil
}

// Only actual immutable payload mutations remain here. Data is validated,
// hashed and owned before entering the writer; no caller source can park it.
func (s *store) writePreparedItems(ctx context.Context, p *preparedStoreSource) error {
	const batch = 4 << 20
	var staged []preparedStoreItem
	var size int
	seen := make(map[[32]byte]bool)
	commit := func() error {
		if len(staged) == 0 {
			return nil
		}
		if err := s.acquireMutation(ctx); err != nil {
			return err
		}
		// Select the current DB under mu: compaction copied protected stages.
		err := s.db.Update(func(tx *bolt.Tx) error {
			items, refs := tx.Bucket(bucketItems), tx.Bucket(bucketItemRefs)
			for _, item := range staged {
				if refs.Get(item.hash[:]) != nil {
					continue
				}
				start := p.file.offset + item.offset
				if err := items.Put(item.hash[:], p.mapped[start:start+int64(item.size)]); err != nil {
					return err
				}
				if err := refs.Put(item.hash[:], append(encoded(int64(item.size)), encoded(0)...)); err != nil {
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
	for _, item := range p.items {
		if err := ctx.Err(); err != nil {
			return err
		}
		if seen[item.hash] {
			continue
		}
		seen[item.hash] = true
		staged = append(staged, item)
		size += item.size
		if size >= batch {
			if err := commit(); err != nil {
				return err
			}
		}
	}
	return commit()
}

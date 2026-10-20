package main

import (
	"encoding/json"

	bolt "go.etcd.io/bbolt"
)

// resolveID locates the tip node for a response alias or socket pin without
// copying any history payload. Returns (tip, charge, found).
func (s *store) resolveID(id string, owner [32]byte, pinned bool) ([32]byte, int64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var tip [32]byte
	var charge int64
	found := false
	_ = s.db.View(func(tx *bolt.Tx) error {
		bucket := bucketResponses
		if pinned {
			bucket = bucketPins
		}
		entry := tx.Bucket(bucket).Get([]byte(id))
		if entry == nil || (!pinned && number(entry[32:40]) <= s.now().UnixNano()) {
			return errMissingItem
		}
		copy(tip[:], entry[:32])
		if pinned {
			charge = number(entry[32:])
		}
		found = true
		return nil
	})
	if found && !pinned {
		// LRU touch; failure must not turn a readable id into a miss.
		_ = s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketResponses)
			v := append([]byte(nil), b.Get([]byte(id))...)
			copy(v[40:], encoded(s.now().UnixNano()))
			return b.Put([]byte(id), v)
		})
	}
	return tip, charge, found
}

// historyHashes walks a tip node collecting item hashes only (32B each).
// Runs one short read tx; heap cost is O(items) metadata, never payloads.
func (s *store) historyHashes(tx *bolt.Tx, node [32]byte, owner [32]byte) ([][32]byte, error) {
	var hashes [][32]byte
	var zero [32]byte
	for n := node; n != zero; {
		v := tx.Bucket(bucketNodes).Get(n[:])
		if len(v) != 104 || string(v[64:96]) != string(owner[:]) {
			return nil, errMissingItem
		}
		var h [32]byte
		copy(h[:], v[32:64])
		hashes = append(hashes, h)
		copy(n[:], v[:32])
	}
	for i, j := 0, len(hashes)-1; i < j; i, j = i+1, j-1 {
		hashes[i], hashes[j] = hashes[j], hashes[i]
	}
	return hashes, nil
}

// writeHistoryTo streams stored item payloads into w in bounded short read
// transactions. Mmap slices are handed to write inside the tx and never
// retained. Returns (items, encoded bytes) for bound checks.
func (s *store) writeHistoryTo(hashes [][32]byte, write func(item []byte) error) (int, int64, error) {
	const batchTx = 8 << 20
	var total int64
	var count int
	for start := 0; start < len(hashes); {
		var batched int64
		end := start
		for end < len(hashes) && batched < batchTx {
			end++
			batched += 1 << 20 // pessimistic; item size unknown until tx
		}
		err := s.db.View(func(tx *bolt.Tx) error {
			its := tx.Bucket(bucketItems)
			for _, h := range hashes[start:end] {
				var zero [32]byte
				if h == zero {
					continue
				}
				item := its.Get(h[:])
				if item == nil {
					return errMissingItem
				}
				if err := write(item); err != nil {
					return err
				}
				total += int64(len(item))
				count++
			}
			return nil
		})
		if err != nil {
			return count, total, err
		}
		start = end
	}
	return count, total, nil
}

// readHistory copies payloads out of mmap inside one read tx. Test/debug
// path only; the hot path uses writeHistoryTo. Returned slices never alias
// database pages.
func readHistory(tx *bolt.Tx, node []byte, owner [32]byte) ([]json.RawMessage, error) {
	var out []json.RawMessage
	var zero [32]byte
	for len(node) > 0 && string(node) != string(zero[:]) {
		v := tx.Bucket(bucketNodes).Get(node)
		if len(v) != 104 || string(v[64:96]) != string(owner[:]) {
			return nil, errMissingItem
		}
		if string(v[32:64]) != string(zero[:]) {
			item := tx.Bucket(bucketItems).Get(v[32:64])
			if item == nil {
				return nil, errMissingItem
			}
			out = append(out, append(json.RawMessage(nil), item...))
		}
		node = v[:32]
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// get resolves an alias/pin and copies history payloads out of mmap inside
// one short read tx. Test/debug path; the hot path uses writeHistoryTo.
func (s *store) get(id string, owner [32]byte) ([]json.RawMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []json.RawMessage
	expired := false
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketResponses).Get([]byte(id))
		if v == nil {
			return errMissingItem
		}
		if number(v[32:40]) <= s.now().UnixNano() {
			expired = true
			return errMissingItem
		}
		var err error
		out, err = readHistory(tx, v[:32], owner)
		return err
	})
	if expired {
		_ = s.update(func(st *storeTx) error { return st.dropResponse([]byte(id)) })
	}
	if err != nil {
		return nil, false
	}
	// A failed LRU touch must not turn an otherwise readable snapshot into a miss.
	_ = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketResponses)
		v := append([]byte(nil), b.Get([]byte(id))...)
		copy(v[40:], encoded(s.now().UnixNano()))
		return b.Put([]byte(id), v)
	})
	return out, true
}

func (s *store) getPinned(pin string, owner [32]byte) ([]json.RawMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []json.RawMessage
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketPins).Get([]byte(pin))
		if v == nil {
			return errMissingItem
		}
		var err error
		out, err = readHistory(tx, v[:32], owner)
		return err
	})
	return out, err == nil
}

// boundedGet keeps the old slice-based bounded lookup for tests.
func (s *store) boundedGet(id string, owner [32]byte, pinned bool) ([]json.RawMessage, bool, error) {
	return s.getLimited(id, owner, pinned, maxHistoryBytes, maxHistoryItems)
}

// Count encoded occurrences, not unique payload bytes: a tiny deduplicated
// store can otherwise expand to an enormous request. Check before copying.
func (s *store) getLimited(id string, owner [32]byte, pinned bool, maxBytes int64, maxItems int) (out []json.RawMessage, found bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	err = s.db.View(func(tx *bolt.Tx) error {
		bucket := bucketResponses
		if pinned {
			bucket = bucketPins
		}
		entry := tx.Bucket(bucket).Get([]byte(id))
		if entry == nil || (!pinned && number(entry[32:40]) <= s.now().UnixNano()) {
			return errMissingItem
		}
		var node [32]byte
		copy(node[:], entry[:32])
		var zero [32]byte
		var size int64 = 2
		count := 0
		for n := node; n != zero; {
			v := tx.Bucket(bucketNodes).Get(n[:])
			if len(v) != 104 || string(v[64:96]) != string(owner[:]) {
				return errMissingItem
			}
			var h [32]byte
			copy(h[:], v[32:64])
			if h != zero {
				r := tx.Bucket(bucketItemRefs).Get(h[:])
				if len(r) != 16 {
					return errMissingItem
				}
				size += number(r[:8]) + 1
				count++
				if size > maxBytes || count > maxItems {
					return errWorkSize
				}
			}
			copy(n[:], v[:32])
		}
		out, err = readHistory(tx, node[:], owner)
		return err
	})
	if err == errMissingItem {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if !pinned {
		_ = s.db.Update(func(tx *bolt.Tx) error {
			b := tx.Bucket(bucketResponses)
			v := append([]byte(nil), b.Get([]byte(id))...)
			copy(v[40:], encoded(s.now().UnixNano()))
			return b.Put([]byte(id), v)
		})
	}
	return out, true, nil
}

// tipHashes resolves a tip node to ordered item hashes under the store mutex.
func (s *store) tipHashes(tip [32]byte, owner [32]byte) ([][32]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var hashes [][32]byte
	err := s.db.View(func(tx *bolt.Tx) error {
		var err error
		hashes, err = s.historyHashes(tx, tip, owner)
		return err
	})
	return hashes, err
}

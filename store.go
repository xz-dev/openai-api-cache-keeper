package main

import (
	"container/list"
	"encoding/json"
	"sync"
	"time"
)

// store keeps the full Responses input history for each stored response id.
// Entries expire a fixed TTL after creation and are evicted least-recently-used
// once the byte budget is exceeded. Nothing is persisted: a miss makes the
// client fall back to sending the full history itself.
type store struct {
	mu       sync.Mutex
	ttl      time.Duration
	maxBytes int64
	used     int64
	entries  map[string]*entry
	lru      *list.List // front = most recently used
	now      func() time.Time
}

type entry struct {
	id      string
	owner   [32]byte
	items   []json.RawMessage
	size    int64
	expires time.Time
	elem    *list.Element
}

func newStore(ttl time.Duration, maxBytes int64) *store {
	return &store{ttl: ttl, maxBytes: maxBytes, entries: map[string]*entry{}, lru: list.New(), now: time.Now}
}

// get returns the history for id when it exists, is unexpired and belongs to
// owner. A foreign id is indistinguishable from a missing one.
func (s *store) get(id string, owner [32]byte) ([]json.RawMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[id]
	if !ok {
		return nil, false
	}
	if !s.now().Before(e.expires) {
		s.remove(e)
		return nil, false
	}
	if e.owner != owner {
		return nil, false
	}
	s.lru.MoveToFront(e.elem)
	return e.items, true
}

func (s *store) put(id string, owner [32]byte, items []json.RawMessage) {
	var size int64
	for _, item := range items {
		size += int64(len(item))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.entries[id]; ok {
		s.remove(old)
	}
	if size > s.maxBytes {
		return
	}
	e := &entry{id: id, owner: owner, items: items, size: size, expires: s.now().Add(s.ttl)}
	e.elem = s.lru.PushFront(e)
	s.entries[id] = e
	s.used += size
	now := s.now()
	for back := s.lru.Back(); back != nil; back = s.lru.Back() {
		oldest := back.Value.(*entry)
		if s.used <= s.maxBytes && now.Before(oldest.expires) {
			break
		}
		s.remove(oldest)
	}
}

func (s *store) remove(e *entry) {
	s.lru.Remove(e.elem)
	delete(s.entries, e.id)
	s.used -= e.size
}

func (s *store) stats() (entries int, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries), s.used
}

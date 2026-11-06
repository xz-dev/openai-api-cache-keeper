package main

import "unsafe"

// History and input item spans have no count cap: they are bounded by bytes
// (the history limit and MAX_REQUEST_BYTES, both the cap) and every span-slice
// capacity is charged to the file's working set before it is allocated
// (design D5).
func (t *diskTurn) addSpan(span itemSpan) error {
	next, free, err := growSpans(t.spans, t.file)
	if err != nil {
		return err
	}
	if free != nil {
		if t.spanFree != nil {
			t.spanFree()
		}
		t.spanFree = free
	}
	t.spans = append(next, span)
	return nil
}

// growSpans returns spans with room for one more element. When it must grow,
// the new capacity is charged to f's working set and the credit is tied to f
// (released when f closes, or earlier through the returned free).
func growSpans(spans []itemSpan, f *spool) ([]itemSpan, func(), error) {
	if len(spans) < cap(spans) {
		return spans, nil, nil
	}
	capacity := max(8, 2*cap(spans))
	free, err := f.mem.reserveInput(int64(capacity) * int64(unsafe.Sizeof(itemSpan{})))
	if err != nil {
		return nil, nil, err
	}
	next := make([]itemSpan, len(spans), capacity)
	copy(next, spans)
	f.mu.Lock()
	f.inputCredits = append(f.inputCredits, free)
	f.mu.Unlock()
	return next, free, nil
}

package main

import "unsafe"

func (t *diskTurn) addSpan(span itemSpan) error {
	if len(t.spans) >= maxHistoryItems {
		return errWorkSize
	}
	if len(t.spans) == cap(t.spans) {
		capacity := min(maxHistoryItems, max(8, 2*cap(t.spans)))
		free, err := t.file.mem.reserveInput(int64(capacity) * int64(unsafe.Sizeof(itemSpan{})))
		if err != nil {
			return err
		}
		next := make([]itemSpan, len(t.spans), capacity)
		copy(next, t.spans)
		if t.spanFree != nil {
			t.spanFree()
		}
		t.spans, t.spanFree = next, free
		t.file.mu.Lock()
		t.file.inputCredits = append(t.file.inputCredits, free)
		t.file.mu.Unlock()
	}
	t.spans = append(t.spans, span)
	return nil
}

package main

import (
	"context"
	"encoding/json"
	"strings"
)

// A prepared acceptance contains no response payload. Close also handles a
// frame that fails to suspend or resume, before its commit can run.
type responseAcceptance struct {
	commit func() (bool, error)
	close  func()
}
type prepareResponseAcceptance func() (*responseAcceptance, error)
type suspendResponseFrame func() (func() error, error)

// Pending bytes are independent immutable file contents, not reclassified heap.
func (k *keeper) savePendingResponse(parts ...[]byte) (*spool, error) {
	var size int64
	for _, p := range parts {
		size += int64(len(p))
	}
	free, err := k.work.reserveInput(size + 512)
	if err != nil {
		return nil, err
	}
	f, err := k.newSpool()
	if err != nil {
		free()
		return nil, err
	}
	f.inputCredits = append(f.inputCredits, free)
	for _, p := range parts {
		if _, err := f.Write(p); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

// Only identity/status survive a terminal acceptance. Copy them under their
// own state lease before dropping all decoded payloads and output credit.
func (c *collector) suspendOutput(m *workingSet) error {
	state, err := m.state(c.idBytes(), nil)
	if err != nil {
		return err
	}
	c.id, c.started, c.status = strings.Clone(c.id), strings.Clone(c.started), strings.Clone(c.status)
	c.items, c.output = nil, nil
	m.freeOutput(c.charged)
	c.charged = 0
	if c.pendingState != nil {
		c.pendingState.Close()
	}
	c.pendingState = state
	return nil
}

func (k *keeper) responseFrameOwned(ctx context.Context, c *collector, body *responseBuffer, event, stored bool, prepare prepareResponseAcceptance, suspend suspendResponseFrame) ([]byte, func(), map[string]json.RawMessage, error) {
	return k.responseFrameProcess(ctx, c, body, true, event, stored, prepare, suspend)
}

func (k *keeper) prepareSocketAcceptance(ctx context.Context, c *collector, turn *diskTurn, owner [32]byte, sess *wsSession, id *string) (*responseAcceptance, error) {
	rid, output, ok := c.final()
	if !ok || *id != "" {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	nextID := rid
	if c.rewrite {
		nextID = publicID(rid)
	}
	freeID, err := sess.reserveID(k.work, nextID)
	if err != nil {
		logNotRetained(err)
		return &responseAcceptance{}, nil
	}
	// reserveID owns this independent copy through commit and socket lifetime.
	if freeID != nil {
		nextID = strings.Clone(nextID)
	} else {
		nextID = sess.lastID
	}
	pending, err := k.prepareAcceptDisk(ctx, turn, publicID(rid), owner, output, sess.lastPin, true)
	if err != nil {
		if freeID != nil {
			freeID()
		}
		logNotRetained(err)
		return &responseAcceptance{}, nil
	}
	return &responseAcceptance{close: func() {
		pending.close()
		if freeID != nil {
			freeID()
			freeID = nil
		}
	}, commit: func() (bool, error) {
		pin, err := pending.commit(ctx)
		if err != nil {
			logNotRetained(err)
			return false, nil
		}
		*id = nextID
		sess.setID(nextID, freeID)
		freeID = nil
		sess.lastPin = pin
		sess.release = func() {
			if err := k.store.release(pin); err != nil {
				logNotRetained(err)
			}
		}
		return true, nil
	}}, nil
}

package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInputFlagsPressureAndStrictValidationRelease(t *testing.T) {
	_, _, k := newTestKeeper(t)
	raw, err := k.spoolRequest(context.Background(), strings.NewReader(`{"input":"`+strings.Repeat("x", 64<<10)+`","stream":true,"background":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	k.work.mu.Lock()
	retained := k.work.input
	k.work.mu.Unlock()
	wanted := inputParseCredit(raw)
	free, err := k.work.reserveInput(maxInputWorkingBytes - wanted + 1)
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = spoolFlags(context.Background(), raw)
	if !errors.Is(err, errWorkSize) {
		t.Fatalf("actual used+requested over budget was not refused: %v", err)
	}
	free()
	stream, background, err := spoolFlags(context.Background(), raw)
	if err != nil || !stream || !background {
		t.Fatalf("fitting input failed: %v %v %v", stream, background, err)
	}
	k.work.mu.Lock()
	remaining := k.work.input
	k.work.mu.Unlock()
	if remaining != retained {
		t.Fatalf("input scratch credit leaked above retained spool: %d want%d", remaining, retained)
	}
	invalid, err := k.spoolRequest(context.Background(), strings.NewReader(`{"input":[{"nested":"\q"}],"stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	defer invalid.Close()
	if _, _, err := spoolFlags(context.Background(), invalid); err == nil {
		t.Fatal("span extraction bypassed nested JSON validation")
	}
	invalid.Close()
	raw.Close()
	k.work.mu.Lock()
	remaining = k.work.input
	k.work.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("final input ownership leaked: %d", remaining)
	}
}

func TestWriteCancellationUnsupportedIsExplicit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if finish, err := interruptCancelledWrites(httptest.NewRecorder(), ctx); !errors.Is(err, http.ErrNotSupported) {
		if finish != nil {
			finish()
		}
		t.Fatalf("unsupported actuator silently accepted: %v", err)
	}
}

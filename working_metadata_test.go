package main

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestRequestMetadataBoundBeforeGrowth(t *testing.T) {
	if _, err := inputItems([]byte("[" + strings.Repeat("{},", maxHistoryItems) + "{}]")); err != errWorkSize {
		t.Fatalf("item count not bounded: %v", err)
	}
	_, _, k := newTestKeeper(t)
	var request strings.Builder
	request.WriteString(`{"input":[]`)
	for i := 0; i < 256; i++ {
		fmt.Fprintf(&request, `,"k%d":0`, i)
	}
	request.WriteString("}")
	raw, err := k.spoolRequest(context.Background(), strings.NewReader(request.String()))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	leave, err := k.work.enter(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer leave()
	turn, apiErr := k.prepareDisk(context.Background(), raw, [32]byte{1}, func(string) ([32]byte, bool) { return [32]byte{}, false }, false)
	if turn != nil {
		turn.file.Close()
		t.Fatal("too many fields accepted")
	}
	if apiErr == nil || apiErr.status != 413 {
		t.Fatalf("field count refusal: %v", apiErr)
	}
}

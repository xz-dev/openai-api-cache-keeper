package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

func (k *keeper) writeUpstreamError(ctx context.Context, conn *websocket.Conn, status int, body []byte) int {
	if err := ctx.Err(); err != nil {
		return wsError(ctx, conn, workingError(err))
	}
	scratch := decodeScratch(len(body))
	if !k.work.reserveOutput(scratch) {
		return wsError(ctx, conn, workingError(errWorkSize))
	}
	var errObj json.RawMessage
	if obj, err := responseObject(body); err == nil && len(obj["error"]) > 0 && obj["error"][0] == '{' {
		errObj = obj["error"]
	}
	if errObj == nil {
		message := strings.TrimSpace(string(body))
		if message == "" {
			message = http.StatusText(status)
		}
		errObj, _ = encodeResponse(map[string]any{"type": "upstream_error", "message": message, "code": nil, "param": nil})
	}
	wire, err := encodeResponse(map[string]any{"type": "error", "status": status, "error": errObj})
	if err != nil || len(wire) > maxResponseBytes {
		k.work.freeOutput(scratch)
		return wsError(ctx, conn, workingError(errWorkSize))
	}
	k.work.freeOutput(scratch - int64(cap(wire)))
	defer k.work.freeOutput(int64(cap(wire)))
	_ = conn.Write(ctx, websocket.MessageText, wire)
	return status
}

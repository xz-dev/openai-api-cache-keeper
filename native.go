package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"strings"
)

// errNativeSocketLimit marks a native WebSocket rejection; it reuses the
// shared working-set error shape.
var errNativeSocketLimit = &apiError{status: http.StatusServiceUnavailable, typ: "server_error", code: "working_set_unavailable", message: "Working capacity unavailable; retry later."}

// newNativeProxy returns the transparent default path: an httputil.ReverseProxy
// to the single configured Sub2API upstream. Unknown paths, methods, queries
// and fields are forwarded unchanged on the keeper transport (no proxy env, no
// compression, no redirect follow). httputil.ReverseProxy streams bodies with
// backpressure, flushes immediately (FlushInterval -1), and tunnels WebSocket
// upgrades itself, including control frames and non-101 handshake failures.
func (k *keeper) newNativeProxy() *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(k.cfg.upstream) // joins the upstream path; sets scheme and Host
			pr.Out.Host = k.cfg.upstream.Host
		},
		Transport:     k.client.Transport,
		FlushInterval: -1,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			log.Printf("native proxy: upstream request failed: %v", err)
			writeAPIError(w, &apiError{status: http.StatusBadGateway, typ: "upstream_error", message: "upstream request failed"})
		},
	}
}

// serveNative is the default path: native HTTP and native WebSocket upgrades
// are proxied transparently by httputil.ReverseProxy. No body is read,
// spooled, or parsed, so native previous_response_id requests stay
// incremental and unrelated traffic keeps its exact semantics.
func (k *keeper) serveNative(w http.ResponseWriter, r *http.Request) {
	if isUpgradeRequest(r) {
		// Native tunnels share the documented live-WebSocket bound.
		if !trySlot(k.work.sockets) {
			writeAPIError(w, errNativeSocketLimit)
			return
		}
		defer func() { <-k.work.sockets }()
	}
	k.native.ServeHTTP(w, r)
}

func isUpgradeRequest(r *http.Request) bool {
	if r.Header.Get("Upgrade") == "" {
		return false
	}
	for _, proto := range r.Header.Values("Connection") {
		for _, item := range strings.Split(proto, ",") {
			if strings.EqualFold(strings.TrimSpace(item), "upgrade") {
				return true
			}
		}
	}
	return false
}

// nativePreviousID extracts the top-level previous_response_id from a request
// body without buffering it, so local checkpoint selection needs no protocol
// flag. ok=false means the field is absent or structurally invalid; those
// requests keep native semantics.
func nativePreviousID(r io.Reader) (string, bool) {
	dec := json.NewDecoder(r)
	dec.UseNumber()
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		return "", false
	}
	for dec.More() {
		key, err := dec.Token()
		if err != nil {
			return "", false
		}
		if name, _ := key.(string); name != "previous_response_id" {
			if _, err := skipJSONValue(dec); err != nil {
				return "", false
			}
			continue
		}
		var id string
		if err := dec.Decode(&id); err != nil || id == "" {
			return "", false
		}
		return id, true
	}
	return "", false
}

// ownedLocalPreviousID reports whether spool's top-level previous_response_id
// is a keeper-owned checkpoint id for owner. It never consults socket pins
// (private, socket-local). A known-but-expired or known-but-foreign id yields
// an apiError so the local parent context is never sent to the native upstream
// nor silently lost; unknown ids are native and pass through.
func (k *keeper) ownedLocalPreviousID(spool *spool, owner [32]byte) (id string, owned bool, apiErr *apiError) {
	id, present := nativePreviousID(io.NewSectionReader(spool.File, spool.offset, spool.size))
	if !present {
		return "", false, nil
	}
	known, valid := k.store.ownedID(id, owner)
	if strings.HasPrefix(id, keeperResponsePrefix) {
		k.cacheMu.Lock()
		task := k.tasks[id]
		k.cacheMu.Unlock()
		if task != nil {
			task.mu.Lock()
			if task.err != nil || task.cancelled || task.expired || !task.complete {
				valid = false
			}
			task.mu.Unlock()
		}
	}
	if !valid && (known || strings.HasPrefix(id, keeperResponsePrefix)) {
		return id, true, &apiError{status: http.StatusBadRequest, typ: "invalid_request_error", code: "previous_response_not_found", param: "previous_response_id",
			message: fmt.Sprintf("Previous response with id '%s' not found.", id)}
	}
	return id, known && valid, nil
}

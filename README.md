# openai-api-cache-keeper

Small Go reverse proxy that keeps OpenAI Responses API history in front of a stateless upstream. Clients send `previous_response_id` plus only the new input; the keeper replays the full history upstream.

```text
API gateway -> openai-api-cache-keeper -> Responses upstream (no server-side state required)
```

The surrounding gateway deployment supplies the public security boundary, authentication, authorization, rate limits and isolation. Do not expose this service directly. It has no admin API, authentication, per-credential ownership in single-upstream mode, or user-selected target. In the front-of-Sub2API deployment below, caller credentials pass through to Sub2API; the deployment, not the keeper, is responsible for the required cache isolation.

## Why

Many OpenAI-compatible upstreams accept `/v1/responses` but ignore or reject `previous_response_id`. Clients then have to resend the whole conversation on every turn, and after a reconnect or a short rollback. The keeper holds that state so the client does not need to.

## Behavior

Cache paths: `POST /v1/responses` (or `/responses`), and `GET /v1/responses` with a WebSocket upgrade. In single-upstream mode other HTTP paths stream through unchanged; routed mode returns 404 for them.

For each request:

1. In routed mode, the keeper looks up the original top-level `model` before accessing history. It selects CPA or AISIX once per HTTP request or WebSocket turn; it never retries against another backend. Single-upstream mode skips classification.
2. If `previous_response_id` is set, the keeper looks it up, prepends the stored history to `input`, and removes the field. An id the keeper stored but can no longer expand (expired, or a failed background checkpoint) returns `400` with `code: "previous_response_not_found"` — fail closed, so a delta is never sent without its history. A `previous_response_id` the keeper never recorded is not an error: in single-upstream mode the request (or WS frame) is forwarded byte-for-byte with the reference intact, because that id may be meaningful to the downstream's own cache; the turn is then downstream-owned and not recorded. In routed mode every turn is local and an unknown id fails with `previous_response_not_found`. Clients can then retry once with the full history.
3. In single-upstream mode every turn whose full input is known — no parent, or a keeper-owned parent — is served locally and recorded like a routed turn: the response id is rewritten to a continuable `resp_` id, `store` is set to `true` when the history is retained, and `input + output` is stored under the returned id. This replaces the earlier passthrough behavior where only checkpoint ids were expanded and nothing else was recorded.
4. The request is forwarded upstream with the caller's headers, except CPA uses the configured `CPA_API_KEY` in routed mode. In routed mode top-level `prompt_cache_retention` is removed on HTTP and WebSocket requests; single-upstream mode preserves it. Nested fields and encoded input items are preserved. Upstream HTTP statuses and error bodies are preserved. Errors are not rewritten, truncated, retried, or put on a cooldown. Successful terminal responses are sent only after any required history storage and socket pin have been acquired.
5. When the upstream finishes with `status: completed` or `incomplete`, the keeper stores `input + output` under the response id. Upstream ids that do not start with `resp_` (for example `msg_…` from Anthropic-style backends) are returned to the client as `resp_<id>` in SSE, JSON, and WebSocket frames, because some gateways only accept `resp_*` as `previous_response_id`. For a turn the keeper stores, the response object is returned with `store: true`, even when the stateless upstream reported `store: false`: the keeper is the store, and clients use that field to decide whether the response id can be continued later. `store: false` requests keep the upstream value unless `IGNORE_CLIENT_STORE_FALSE=true`.

What is stored:

- Raw Responses items exactly as received or returned, including reasoning `encrypted_content`, `function_call`, and `function_call_output`. No format conversion is done.
- Each entry is the full history up to that turn. Any stored id can be continued, so rolling back one or two steps is a normal continuation from an older id.
- In single-upstream mode there is no per-credential isolation: every stored id is continuable by any caller, because the surrounding deployment provides the required isolation. Do not expose the keeper directly or treat unguessable response ids as authorization. In routed mode entries remain scoped to the SHA-256 of the credential Sub2API authenticates after hop-by-hop headers are removed: a case-insensitive `Bearer` token (trimmed), else `X-API-Key`, else `X-Goog-API-Key`. The digest only isolates entries; every task read or cancel is still revalidated upstream.
- `store: false` requests are not stored. On a WebSocket, the most recent turn can still be continued on the same socket, like OpenAI's connection-local cache. With `IGNORE_CLIENT_STORE_FALSE=true` they are stored (and returned with `store: true`) like any other turn, for clients such as Codex that always send `store: false` but should still recover after a reconnect.

Retained history is bounded by the configured response-ID count, payload and metadata budgets. Entries expire `STORE_TTL` after creation/replacement; reads do not extend that deadline. Both the count and payload limits evict registered IDs by least recent access (LRU), protecting the incoming ID. Socket pins and shared prefix nodes do not count as entries; evicting an alias does not invalidate an active pin. bbolt under `STORE_DIR` holds both content-addressed items and transactional references/accounting. Identical item bytes are stored once. Per-item prefix nodes share growing histories even when clients resend full history without a previous ID; no per-response growing hash manifest is retained. `STORE_META_MAX_BYTES` charges every prefix node/hash occurrence, unique item index, response alias (including ID length), and socket pin. `STORE_PIN_MAX_BYTES` bounds all live WebSocket pins, including registered turns kept alive on their original socket; each socket charges its history's unique payload bytes. Replacement uses the net pin charge, not old plus new.

The cache is an optimization, never a reason to fail a turn. When history cannot be retained (budget, pin, compaction or I/O failure), the turn is still served in full and logged as `history not retained`; its terminal response keeps the upstream `store` value instead of being advertised as `store: true`, and continuing from that id returns `previous_response_not_found`, so the client resends full history. Previous accepted history, including the socket's last pin, remains intact. Keeper-side mid-stream failures (truncated upstream stream, working-set limit) end the turn in protocol terms: on SSE and WebSocket a turn whose upstream response already started (`response.created`/`response.in_progress`, or a collected terminal id) ends with `response.failed` (same public id, `status: failed`, the error in `response.error`); a turn that never started gets a request-level `error` event/frame (WebSocket frames carry `status`). Clients and any gateway in front therefore see every turn terminate without waiting for a timeout, and the WebSocket stays open for the next turn. Intermediate stream events may already have been delivered. A failed cleanup transaction retains its charged records rather than pretending bytes were reclaimed.

`STORE_COMPACT_BYTES` is a **maintenance trigger, not a hard disk quota**. Before each admission, expiry runs and a file above the trigger is compacted using bbolt's bounded-transaction compactor. One admission may take the file above the trigger; a live set larger than the trigger cannot shrink below it. Budget disk headroom for the original high-water file **plus a compacted copy**, bbolt page overhead and copy-on-write growth. A required compaction failure leaves the incoming turn uncached while the original database continues serving accepted references. Neither a payload limit nor a compaction trigger is an exact filesystem-byte limit.

Nothing survives a restart: the cache is disposable and cleared on startup. A stable directory lock prevents simultaneous keepers, including during database file replacement. Corrupt startup fails safely without removing the database; an operator can remove the disposable cache only after stopping its owner. After restart or eviction, continuation returns `previous_response_not_found` so the client can resend full history. `STORE_DIR` must be a real filesystem-backed directory (never tmpfs): mmap pages count against the container memory limit. Request/response transient allocations and admission/load defenses are separate from these retained-cache budgets.

### Transient working-set limits

Requests and expanded upstream bodies use private marked files inside `STORE_DIR/.keeper-spools`, not idle-socket or slow-stream heap buffers. A random namespace owner marker is copied into each spool header; cleanup checks regular-file type without following symlinks and removes only matching owned files. Foreign matching-prefix files, symlinks and directories are preserved. Files are bounded and removed after both turn completion/cancellation **and** transport `Request.Body.Close`; bytes remain charged until actual cleanup. Stale owned files are removed at startup while the store directory lock is held. This is transient transport buffering, not restart persistence. Spool disk write failures (the request body itself) are explicit turn failures; failed unlink keeps bytes charged and is logged.

For the measured 384-MiB memory envelope, the following conservative limits are currently fixed (not additional environment knobs):

| Resource | Limit / behavior |
|---|---|
| Client request/frame | Existing `MAX_REQUEST_BYTES`, default 32 MiB; unchanged. Spool before JSON decode. |
| In-flight/queued turns | 64 total across HTTP and WS; overflow returns explicit `working_set_unavailable` (503). |
| Live WebSockets | 64; excess handshake rejected with 503. |
| Pending WS messages | One queued message per socket in addition to current turn; excess messages receive explicit overload. Reader keeps servicing control frames while work queues. |
| Materialization | One short decode/expand, terminal-storage or response parse/rewrite phase at a time, with at most 8 seconds waiting, cancellable. **Not** a whole-turn inference semaphore or a byte allocator. No phase spans network reads/writes. |
| Encoded history/upstream request | 64 MiB and 8,192 history items. Request objects permit at most 256 field occurrences and 64-KiB field names; array items and fields are checked incrementally before growing metadata. Repeated deduplicated items count on every occurrence before history copying. No HTML-escaping inflation of input RawMessages. |
| SSE event / total stream | 1 MiB per complete event (including multiline/raw framing); 8 MiB cumulative upstream stream. |
| Non-stream successful response / per-turn collection | 8 MiB. HTTP upstream error bodies are copied without retention; WS error bodies exceeding 8 MiB fail explicitly instead of being truncated. |
| Response metadata | At most 256 fields per parsed response/event object, 64-KiB field names, 8,192 output items; checked incrementally before collection allocation, including `store:false`. Retained slice capacity and item capacities are charged. |
| Global response reservations | 16 MiB shared by raw reader/frame/body capacities, buffer-growth overlap, retained output/metadata, parse/rewrite scratch and encoded wire buffers. Scratch reserves `8 × source bytes + 2 × 8192 × 24 + 256 × 128` before parsing/rewriting; pressure may reject below individual size maxima. Wire capacity stays charged until network write returns. |
| Transient disk files | 1 GiB aggregate, charged before writes; no tmpfs. |

Size refusals use `working_set_too_large` (413); exhausted/expired work or transient disk capacity uses `working_set_unavailable` (503). SSE/WS emit structured errors before terminal success; intermediate deltas may already have arrived. Storage acceptance and atomic previous-pin preservation are unchanged. SSE uses a flat charged frame buffer with no per-line collection; only a blank-line-delimited event can dispatch, never pending bytes on a read error. Malformed JSON or invalid completed/incomplete objects produce explicit upstream errors; valid asynchronous JSON responses still pass through. Rewriting disables HTML escaping rather than amplifying `<>&` payloads. Active stored/cache references are not deleted to disguise a rejected turn.

These limits remove known concurrent full-history/body retention. They do not make `GOMEMLIMIT` a hard cap or exclude mmap/dirty/file-cache pages from cgroup memory. Local diagnostic testing uses real 384-MiB memory, no swap, `GOMEMLIMIT=256MiB`, and a separately declared 512-MiB cache profile. That cache profile is **not** a recommendation to expand production storage. CPU quota enforcement and the full gateway protocol chain still need independent acceptance; no new deployment is authorized by local memory results.

### In front of Sub2API

In the gateway chain the keeper sits **in front of** Sub2API, behind the public APISIX/keepalive hop: its `UPSTREAM_URL` is the `responses-ws-sse-bridge` service, which converts WebSocket turns to HTTP and forwards to Sub2API (authentication, quotas and account selection). The account's `base_url` does not point at the keeper; the keeper owns WS termination for the whole chain. See [the AI-gateway decision and deployment record](https://github.com/xz-dev/AI-gateway/blob/main/docs/websocket-termination.md) for the deployed architecture.

`IGNORE_CLIENT_STORE_FALSE=true` is **required** in this placement. Clients such as Codex send `store:false` on every turn; without the flag those turns are only pinned to their socket, so reconnecting loses all history. With the flag every turn — HTTP or native WS — registers in the shared store, is returned with `store:true`, and continues on a new socket or over HTTP. This is the same decision the HTTP path makes at `prepareDiskEnv` (working.go): the flag forces `t.store = true`, which is shared by both transports.

The earlier Sub2API `passthrough` → keeper → bridge placement failed because a request error poisoned Sub2API's socket lifecycle and WS-created ids could not continue over HTTP. Passthrough also injects `store:false`, but the initial reconnect failure was a stale keeper process without the flag, not a missing WS implementation.

### WebSocket mode

Routed mode terminates the socket: each `response.create` frame is expanded like an HTTP request, sent upstream as streaming HTTP, and every upstream SSE event returns as one text frame.

Single-upstream mode keeps WebSocket end to end: the keeper accepts the client socket, dials a second socket to the upstream with the client's headers (minus hop-by-hop and `Sec-WebSocket-*`), and relays every frame. Only `response.create` text frames are inspected, once per turn:

- A `previous_response_id` the keeper recorded is expanded to the full stored history and the field is removed, so the downstream only sees a self-contained request. The turn's events are collected and rewritten like an HTTP turn, and the produced id stays continuable on the same socket (pin) or a new one (store).
- No parent: the frame is forwarded semantically unchanged and the turn is recorded, so its id becomes continuable.
- Any other `previous_response_id` is downstream-native: the frame and its response frames relay byte-for-byte, unrecorded and unrewritten. `generate:false` prewarm turns with no parent or a keeper-owned parent are recorded too.

Turns serialize in order; while a turn is in flight the socket queues at most one further `response.create`. Other frames — upstream events, non-create client messages, control frames — relay immediately. A non-101 upstream handshake is passed through with its status and body. Client close cancels the upstream request.

### Turn logs

Each native WS `response.create` logs exactly once at its terminal event, local rejection, or transport abort, not when its frame is forwarded. `duration` includes upstream streaming and storage acceptance; `previous` records whether the request supplied a parent. `stored=true` means the turn was accepted into the cache (registered or socket-pinned); `stored=false` means it was not retained. An accepted public id is passed internally to the logger, but the existing log format deliberately includes no response-id or payload field.

`status` is the HTTP/relay outcome, as on the HTTP path: a delivered terminal protocol error can have `status=200 stored=false`; inspect the protocol error for its application code. A transport-aborted turn logs `status=502`. Opaque/non-create frames do not produce turn logs. Cache and wire behavior are unchanged by this logging fix.

### Capacity

Recording every turn (previously only checkpoint/background turns were recorded) increases store pressure with inference volume. Defaults are `STORE_TTL=10m`, `STORE_MAX_ENTRIES=100` and `STORE_MAX_BYTES=0`. Zero means unlimited for **each of those two limits only**: it does not disable expiry, metadata/pin budgets, compaction, or request/working-set limits. The first exhausted limit can evict an alias before its TTL. Each live WebSocket also pins its last accepted turn (`STORE_PIN_MAX_BYTES`), unchanged from before.

A diagnostic profile is `STORE_TTL=24h`, `STORE_MAX_ENTRIES=10000`, `STORE_MAX_BYTES=0`; it is not the default or an unconditional production recommendation. TTL only applies within one process lifetime: startup wipes every alias and does not restore conversations. The cache holds plaintext request/response history, potentially for 24 hours under that profile. Expiry, eviction and startup clearing are logical deletions, not secure erasure of old filesystem/database pages.

Size by unique retained item bytes, prefix/alias overhead, bbolt high-water space, **an additional compacted copy**, and transient spool headroom. Count alone is not a disk bound: 10,000 independent 512-KiB inputs already total about 4.9 GiB before outputs or overhead. Monitor filesystem free space and memory; use a positive `STORE_MAX_BYTES` or a dedicated larger volume when that headroom is unavailable. `STORE_MAX_BYTES` bounds live payload, not exact filesystem usage. See [the bounded 24h/10,000/0 experiment](docs/cache-limits-20261023.md) for measurements, failure injection and limitations.

## Configuration

Choose one upstream mode:

- **Single upstream:** set `UPSTREAM_URL` only.
- **Routed:** unset `UPSTREAM_URL`; set `ROUTING_INDEX_URL`, `UPSTREAM_CPA_URL`, `UPSTREAM_AISIX_URL`, and `CPA_API_KEY` together. Supply the key through the runtime secret environment. Partial or mixed configurations fail startup. URL values must use HTTP(S), without userinfo, query, or fragment.

Routing performs one `GET` to `ROUTING_INDEX_URL` with the exact model ID as the `model` query parameter. The model must be a non-empty string of at most 1,024 UTF-8 bytes. Lookup limits are 300 ms connect, 300 ms send, 1 s read, 1,024 response-body bytes, and a 1.6 s overall ceiling. Redirects and implicit connection-reuse retries are disabled. Only `cpa`, `aisix`, `not_found`, and `unavailable` are accepted, with a nonnegative integral numeric generation; generation zero is valid only for `unavailable`.

Each keeper process retains at most 4,096 successful decisions, evicting the oldest first. A transport, non-200, or malformed-response failure may use a decision less than 15 minutes old for that exact model; a fallback does not refresh its age. Authoritative `not_found` or `unavailable` deletes the entry. Each fallback logs the model and age, without credentials. The routing map is never persisted.

An invalid model or malformed request returns `400 / invalid_request_error`; `not_found` returns `404 / model_not_found`; `unavailable` or lookup failure without a fresh fallback returns `503 / service_unavailable`. Non-stream HTTP uses JSON. HTTP requests with `stream:true` use an SSE `error` event with the same non-2xx status. WebSocket turns receive an `error` frame carrying that status; the socket and previous accepted history remain usable. Routing errors do not reach a backend or access stored history.

| Variable | Default | Meaning |
|---|---|---|
| `UPSTREAM_URL` | required in single mode | Upstream base URL, e.g. `http://cpa:8317`. The request path is appended. |
| `ROUTING_INDEX_URL` | required in routed mode | Full routing-index endpoint, e.g. `http://models-enricher:8090/routing-index`. |
| `UPSTREAM_CPA_URL` | required in routed mode | Direct CPA base URL, e.g. `http://cpa:8317`. The request path is appended. |
| `UPSTREAM_AISIX_URL` | required in routed mode | Direct AISIX base URL, e.g. `http://aisix:3000`. The request path is appended. |
| `CPA_API_KEY` | required in routed mode | Non-empty single-line runtime secret. CPA receives `Authorization: Bearer <key>`; AISIX retains the caller header. |
| `LISTEN_ADDR` | `:8080` | Listen address. |
| `STORE_TTL` | `10m` | Positive Go duration; lifetime from entry creation/replacement, not last read. |
| `STORE_MAX_ENTRIES` | `100` | Maximum registered response-ID aliases; LRU eviction. `0` is unlimited; negative values fail startup. Pins/shared history nodes are excluded. |
| `STORE_MAX_BYTES` | `0` | Live retained history payload budget, not a filesystem quota; LRU eviction. `0` is unlimited; negative values fail startup. Other safety limits remain. |
| `MAX_REQUEST_BYTES` | `33554432` | Maximum request body or WebSocket frame size. |
| `STORE_DIR` | `/var/tmp/openai-api-cache-keeper` | Directory for the disposable disk cache. Must be a real disk volume, never tmpfs. |
| `STORE_META_MAX_BYTES` | `33554432` | Accounted metadata budget for prefix nodes, unique item indexes, response aliases and socket pins. |
| `STORE_PIN_MAX_BYTES` | `104857600` | Aggregate payload budget for all live WebSocket history pins. |
| `STORE_COMPACT_BYTES` | `419430400` | Physical file maintenance trigger before admission, not a hard disk quota; temporary-copy headroom required. |
| `REQUIRE_NO_DEFAULT_ROUTE` | `false` | Refuse to start while the network namespace has a default route. |
| `IGNORE_CLIENT_STORE_FALSE` | `false` | Store turns even when the client sent `store: false`. |

`/healthz` returns `200 ok`; `openai-api-cache-keeper healthcheck` probes it for container health checks.

## Build and test

```sh
go test -race ./...
docker build -t openai-api-cache-keeper .
```

`testdata/routing-index.json` is the parser contract fixture. `TestRoutingIndexContract` checks Go; `testdata/routing-index.lua` checks the same cases against an unmodified `routing_selector.lua` using the APISIX image's `resty` executable (arguments: selector path, JSON fixture path).

CI publishes `ghcr.io/xz-dev/openai-api-cache-keeper` with `latest` and the full commit SHA as tags on every push to `main`. Deployments should pin a digest.

## License

MIT

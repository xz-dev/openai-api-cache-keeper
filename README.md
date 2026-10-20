# response-keeper

Small Go reverse proxy that keeps OpenAI Responses API history in front of a stateless upstream. Clients send `previous_response_id` plus only the new input; the keeper replays the full history upstream.

```text
API gateway -> response-keeper -> Responses upstream (no server-side state required)
```

The API gateway in front remains the public and security boundary. It must authenticate, authorize, rate-limit, and validate requests before forwarding here. This service has no admin API, metrics, or user-selected target.

## Why

Many OpenAI-compatible upstreams accept `/v1/responses` but ignore or reject `previous_response_id`. Clients then have to resend the whole conversation on every turn, and after a reconnect or a short rollback. The keeper holds that state so the client does not need to.

## Behavior

Paths: `POST /v1/responses` (or `/responses`), and `GET /v1/responses` with a WebSocket upgrade. Everything else returns 404.

For each request:

1. In routed mode, the keeper looks up the original top-level `model` before accessing history. It selects CPA or AISIX once per HTTP request or WebSocket turn; it never retries against another backend. Single-upstream mode skips classification.
2. If `previous_response_id` is set, the keeper looks it up, prepends the stored history to `input`, and removes the field. An unknown, expired, or foreign id returns `400` with `code: "previous_response_not_found"`. Clients can then retry once with the full history.
3. The request is forwarded upstream with the caller's headers, except CPA uses the configured `CPA_API_KEY` in routed mode. Top-level `prompt_cache_retention` is removed on HTTP and WebSocket requests; nested fields and encoded input items are preserved. Upstream HTTP statuses and error bodies are preserved. Errors are not rewritten, truncated, retried, or put on a cooldown. Successful terminal responses are sent only after any required history storage and socket pin have been acquired.
4. When the upstream finishes with `status: completed` or `incomplete`, the keeper stores `input + output` under the response id. Upstream ids that do not start with `resp_` (for example `msg_…` from Anthropic-style backends) are returned to the client as `resp_<id>` in SSE, JSON, and WebSocket frames, because some gateways only accept `resp_*` as `previous_response_id`. For a turn the keeper stores, the response object is returned with `store: true`, even when the stateless upstream reported `store: false`: the keeper is the store, and clients use that field to decide whether the response id can be continued later. `store: false` requests keep the upstream value.

What is stored:

- Raw Responses items exactly as received or returned, including reasoning `encrypted_content`, `function_call`, and `function_call_output`. No format conversion is done.
- Each entry is the full history up to that turn. Any stored id can be continued, so rolling back one or two steps is a normal continuation from an older id.
- Entries are scoped to the SHA-256 of the incoming `Authorization` header, before any CPA credential substitution. If a gateway forwards one shared upstream key for every user, entries are not isolated per user.
- `store: false` requests are not stored. On a WebSocket, the most recent turn can still be continued on the same socket, like OpenAI's connection-local cache. With `IGNORE_CLIENT_STORE_FALSE=true` they are stored (and returned with `store: true`) like any other turn, for clients such as Codex that always send `store: false` but should still recover after a reconnect.

Retained history is bounded by payload and metadata budgets. Entries expire `STORE_TTL` after creation; least-recently-used registered IDs can be evicted to make room. bbolt under `STORE_DIR` holds both content-addressed items and transactional references/accounting. Identical item bytes are stored once. Per-item prefix nodes share growing histories even when clients resend full history without a previous ID; no per-response growing hash manifest is retained. `STORE_META_MAX_BYTES` charges every prefix node/hash occurrence, unique item index, response alias (including ID length), and socket pin. `STORE_PIN_MAX_BYTES` bounds all live WebSocket pins, including registered turns kept alive on their original socket; each socket charges its history's unique payload bytes. Replacement uses the net pin charge, not old plus new.

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

### Behind Sub2API

Point an OpenAI API-key account's `base_url` at the keeper and use WebSocket mode `ctx_pool`, not `passthrough`: passthrough does not bind response ids to the account, so a new socket drops `previous_response_id`. Also set `GATEWAY_OPENAI_WS_INGRESS_PREVIOUS_RESPONSE_RECOVERY_ENABLED=false`. Otherwise, when the keeper has lost an id, Sub2API silently retries without `previous_response_id` and the turn loses its context instead of returning `previous_response_not_found`.

### WebSocket mode

Each text frame must be a `response.create` event; its body is handled like an HTTP streaming request. Every upstream SSE event is sent back as one text frame. Upstream HTTP errors are sent as an `error` frame with the upstream `status` and its original `error` object. Turns on one socket run in order. Closing the socket cancels the in-flight upstream request.

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
| `STORE_TTL` | `10m` | Lifetime of a stored entry. |
| `STORE_MAX_BYTES` | `209715200` | Byte budget for stored history (LRU eviction). |
| `MAX_REQUEST_BYTES` | `33554432` | Maximum request body or WebSocket frame size. |
| `STORE_DIR` | `/var/tmp/response-keeper` | Directory for the disposable disk cache. Must be a real disk volume, never tmpfs. |
| `STORE_META_MAX_BYTES` | `33554432` | Accounted metadata budget for prefix nodes, unique item indexes, response aliases and socket pins. |
| `STORE_PIN_MAX_BYTES` | `104857600` | Aggregate payload budget for all live WebSocket history pins. |
| `STORE_COMPACT_BYTES` | `419430400` | Physical file maintenance trigger before admission, not a hard disk quota; temporary-copy headroom required. |
| `REQUIRE_NO_DEFAULT_ROUTE` | `false` | Refuse to start while the network namespace has a default route. |
| `IGNORE_CLIENT_STORE_FALSE` | `false` | Store turns even when the client sent `store: false`. |

`/healthz` returns `200 ok`; `response-keeper healthcheck` probes it for container health checks.

## Build and test

```sh
go test -race ./...
docker build -t response-keeper .
```

`testdata/routing-index.json` is the parser contract fixture. `TestRoutingIndexContract` checks Go; `testdata/routing-index.lua` checks the same cases against an unmodified `routing_selector.lua` using the APISIX image's `resty` executable (arguments: selector path, JSON fixture path).

CI publishes `ghcr.io/xz-dev/response-keeper` with `latest` and the full commit SHA as tags on every push to `main`. Deployments should pin a digest.

## License

MIT

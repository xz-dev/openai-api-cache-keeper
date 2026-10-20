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

1. If `previous_response_id` is set, the keeper looks it up, prepends the stored history to `input`, and removes the field. An unknown, expired, or foreign id returns `400` with `code: "previous_response_not_found"`. Clients can then retry once with the full history.
2. The request is forwarded upstream with the caller's headers. Upstream HTTP statuses and error bodies are preserved. Errors are not rewritten, truncated, retried, or put on a cooldown. Successful terminal responses are sent only after any required history storage and socket pin have been acquired.
3. When the upstream finishes with `status: completed` or `incomplete`, the keeper stores `input + output` under the response id. Upstream ids that do not start with `resp_` (for example `msg_…` from Anthropic-style backends) are returned to the client as `resp_<id>` in SSE, JSON, and WebSocket frames, because some gateways only accept `resp_*` as `previous_response_id`. For a turn the keeper stores, the response object is returned with `store: true`, even when the stateless upstream reported `store: false`: the keeper is the store, and clients use that field to decide whether the response id can be continued later. `store: false` requests keep the upstream value.

What is stored:

- Raw Responses items exactly as received or returned, including reasoning `encrypted_content`, `function_call`, and `function_call_output`. No format conversion is done.
- Each entry is the full history up to that turn. Any stored id can be continued, so rolling back one or two steps is a normal continuation from an older id.
- Entries are scoped to the SHA-256 of the `Authorization` header. If a gateway forwards one shared upstream key for every user, entries are not isolated per user.
- `store: false` requests are not stored. On a WebSocket, the most recent turn can still be continued on the same socket, like OpenAI's connection-local cache. With `IGNORE_CLIENT_STORE_FALSE=true` they are stored (and returned with `store: true`) like any other turn, for clients such as Codex that always send `store: false` but should still recover after a reconnect.

Retained history is bounded by payload and metadata budgets. Entries expire `STORE_TTL` after creation; least-recently-used registered IDs can be evicted to make room. bbolt under `STORE_DIR` holds both content-addressed items and transactional references/accounting. Identical item bytes are stored once. Per-item prefix nodes share growing histories even when clients resend full history without a previous ID; no per-response growing hash manifest is retained. `STORE_META_MAX_BYTES` charges every prefix node/hash occurrence, unique item index, response alias (including ID length), and socket pin. `STORE_PIN_MAX_BYTES` bounds all live WebSocket pins, including registered turns kept alive on their original socket; each socket charges its history's unique payload bytes. Replacement uses the net pin charge, not old plus new.

Storage or replacement-pin failure rejects the **current** turn with `history_storage_failed`: JSON HTTP returns `503`; SSE emits an `error` event; WebSocket emits an `error` frame with status `503`. No completed/incomplete terminal success or `[DONE]` is sent for a rejected turn. Intermediate stream events may already have been delivered. Previous accepted socket history remains intact. A failed cleanup transaction retains its charged records rather than pretending bytes were reclaimed.

`STORE_COMPACT_BYTES` is a **maintenance trigger, not a hard disk quota**. Before each admission, expiry runs and a file above the trigger is compacted using bbolt's bounded-transaction compactor. One admission may take the file above the trigger; a live set larger than the trigger cannot shrink below it. Budget disk headroom for the original high-water file **plus a compacted copy**, bbolt page overhead and copy-on-write growth. A required compaction failure rejects the incoming turn while the original database continues serving accepted references. Neither a payload limit nor a compaction trigger is an exact filesystem-byte limit.

Nothing survives a restart: the cache is disposable and cleared on startup. A stable directory lock prevents simultaneous keepers, including during database file replacement. Corrupt startup fails safely without removing the database; an operator can remove the disposable cache only after stopping its owner. After restart or eviction, continuation returns `previous_response_not_found` so the client can resend full history. `STORE_DIR` must be a real filesystem-backed directory (never tmpfs): mmap pages count against the container memory limit. Request/response transient allocations and admission/load defenses are separate from these retained-cache budgets.

### Transient working-set limits

Requests and expanded upstream bodies use private marked files inside `STORE_DIR/.keeper-spools`, not idle-socket or slow-stream heap buffers. A random namespace owner marker is copied into each spool header; cleanup checks regular-file type without following symlinks and removes only matching owned files. Foreign matching-prefix files, symlinks and directories are preserved. Files are bounded and removed after both turn completion/cancellation **and** transport `Request.Body.Close`; bytes remain charged until actual cleanup. Stale owned files are removed at startup while the store directory lock is held. This is transient transport buffering, not restart persistence. Disk write failures are explicit turn failures; failed unlink keeps bytes charged and is logged.

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

| Variable | Default | Meaning |
|---|---|---|
| `UPSTREAM_URL` | required | Upstream base URL, e.g. `http://cpa:8317`. The request path is appended. |
| `LISTEN_ADDR` | `:8080` | Listen address. |
| `STORE_TTL` | `10m` | Lifetime of a stored entry. |
| `STORE_MAX_BYTES` | `134217728` | Byte budget for stored history (LRU eviction). |
| `MAX_REQUEST_BYTES` | `33554432` | Maximum request body or WebSocket frame size. |
| `STORE_DIR` | `/var/tmp/response-keeper` | Directory for the disposable disk cache. Must be a real disk volume, never tmpfs. |
| `STORE_META_MAX_BYTES` | `33554432` | Accounted metadata budget for prefix nodes, unique item indexes, response aliases and socket pins. |
| `STORE_PIN_MAX_BYTES` | `67108864` | Aggregate payload budget for all live WebSocket history pins. |
| `STORE_COMPACT_BYTES` | `335544320` | Physical file maintenance trigger before admission, not a hard disk quota; temporary-copy headroom required. |
| `REQUIRE_NO_DEFAULT_ROUTE` | `false` | Refuse to start while the network namespace has a default route. |
| `IGNORE_CLIENT_STORE_FALSE` | `false` | Store turns even when the client sent `store: false`. |

`/healthz` returns `200 ok`; `response-keeper healthcheck` probes it for container health checks.

## Build and test

```sh
go test -race ./...
docker build -t response-keeper .
```

CI publishes `ghcr.io/xz-dev/response-keeper` with `latest` and the full commit SHA as tags on every push to `main`. Deployments should pin a digest.

## License

MIT

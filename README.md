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
2. The request is forwarded upstream with the caller's headers. The upstream status, headers, and body are relayed unchanged. Errors are not rewritten, truncated, retried, or put on a cooldown.
3. When the upstream finishes with `status: completed` or `incomplete`, the keeper stores `input + output` under the response id. Upstream ids that do not start with `resp_` (for example `msg_…` from Anthropic-style backends) are returned to the client as `resp_<id>` in SSE, JSON, and WebSocket frames, because some gateways only accept `resp_*` as `previous_response_id`. For a turn the keeper stores, the response object is returned with `store: true`, even when the stateless upstream reported `store: false`: the keeper is the store, and clients use that field to decide whether the response id can be continued later. `store: false` requests keep the upstream value.

What is stored:

- Raw Responses items exactly as received or returned, including reasoning `encrypted_content`, `function_call`, and `function_call_output`. No format conversion is done.
- Each entry is the full history up to that turn. Any stored id can be continued, so rolling back one or two steps is a normal continuation from an older id.
- Entries are scoped to the SHA-256 of the `Authorization` header. If a gateway forwards one shared upstream key for every user, entries are not isolated per user.
- `store: false` requests are not stored. On a WebSocket, the most recent turn can still be continued on the same socket, like OpenAI's connection-local cache.

Memory is bounded: entries expire `STORE_TTL` after creation, and the least recently used entries are evicted once `STORE_MAX_BYTES` is exceeded. Nothing is persisted. After a restart or an eviction, the next continuation returns `previous_response_not_found` and the client resends the full history once.

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
| `REQUIRE_NO_DEFAULT_ROUTE` | `false` | Refuse to start while the network namespace has a default route. |

`/healthz` returns `200 ok`; `response-keeper healthcheck` probes it for container health checks.

## Build and test

```sh
go test -race ./...
docker build -t response-keeper .
```

CI publishes `ghcr.io/xz-dev/response-keeper` with `latest` and the full commit SHA as tags on every push to `main`. Deployments should pin a digest.

## License

MIT

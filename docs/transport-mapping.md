# How portway maps stdio to Streamable HTTP (and back)

portway is a pure transport adapter between the two standard MCP
transports. The JSON-RPC messages themselves are never rewritten — only
the wire framing around them changes. This document is the exact
contract, for both directions.

## The two transports in one paragraph each

**stdio**: the server is a child process; the client writes
newline-delimited JSON-RPC messages to its stdin and reads them from its
stdout. One process is one conversation. Server-initiated messages are
simply more lines on stdout.

**Streamable HTTP**: the server is one HTTP endpoint. The client POSTs
each message; requests are answered in the POST response body (as
`application/json` or a `text/event-stream`), while notifications and
responses get `202 Accepted`. A separate GET on the same endpoint opens
a Server-Sent-Events stream for server-initiated messages. A session id
assigned at `initialize` (the `Mcp-Session-Id` header) ties it together,
and `DELETE` ends it.

## `portway serve` — stdio server, HTTP clients

| HTTP request | portway behavior |
| --- | --- |
| `POST` initialize request | (re)spawns the child, forwards the message, waits for the response frame, returns it as `application/json` with a fresh `Mcp-Session-Id` |
| `POST` other request | forwards to the child's stdin; the response frame with the matching id is returned as `application/json` |
| `POST` notification / response | forwards to the child's stdin, returns `202 Accepted` |
| `GET` | the single SSE stream: every child stdout frame that is *not* a response to an HTTP request (server requests, notifications, orphan responses) is delivered here with a monotonically increasing `id:` field |
| `GET` with `Last-Event-ID` | replays buffered messages after that id (buffer size: `--buffer`, default 256), then continues live |
| `DELETE` | stops the child, ends the session, `204 No Content` |

Status codes are part of the contract: `400` for malformed bodies or a
missing session header, `404` for a stale session id (the spec's
"reinitialize" signal), `405` with `Allow` for other methods, `409` for
a second concurrent GET stream or a duplicate in-flight request id, and
`502` when the child exits before answering.

Design decisions worth knowing:

- **One child, one session.** A stdio server is inherently a single
  conversation, so the bridge holds exactly one session. A new
  `initialize` replaces the old session and respawns the child — a
  restarted client is never locked out.
- **POST responses are always `application/json`.** The spec lets a
  server choose SSE per POST; portway cannot know which child
  notifications "belong" to which request, so it keeps request/response
  on the POST and everything else on the GET stream. (Per-request SSE
  with `progressToken` affinity is on the roadmap.)
- **Bodies are compacted** before touching the child's stdin: HTTP
  bodies may be pretty-printed, but an NDJSON wire cannot carry embedded
  newlines.
- **JSON-RPC batches are rejected** (`400`); they were removed from the
  transport in protocol revision 2025-06-18 and cannot be translated
  faithfully.

## `portway connect` — HTTP server, stdio client

| stdin frame | portway behavior |
| --- | --- |
| initialize request | `POST`ed synchronously; the `Mcp-Session-Id` response header and `result.protocolVersion` are captured, then the GET listener starts |
| other request | `POST`ed **concurrently** — a slow tool call must not block the client from answering a server-initiated request (e.g. sampling) |
| notification / response | `POST`ed in stdin order; a `202` is expected and discarded |

Whatever the server sends back — a JSON body, an SSE response stream, or
messages on the standalone GET stream — is written to stdout as
newline-delimited JSON (compacted, since SSE data may span lines). The
GET listener reconnects with `Last-Event-ID` if the stream drops, and
gives up quietly on `405` (servers without a stream are legal). On stdin
EOF portway drains in-flight requests, keeps the GET stream open for a
short grace window (200 ms) so a notification sent right after the final
response is not lost, sends a best-effort `DELETE`, and exits 0.

**Failures are answered, never swallowed.** A stdio client has no HTTP
status codes to look at, so any HTTP-level failure of a *request* — a
connection error, a 4xx/5xx, a 202 with no response, an SSE stream that
ends early — is synthesized into a JSON-RPC error response (code
`-32000`, message prefixed `portway:`) with the original request id.
Notifications get no synthesized answer (nothing may answer them, per
JSON-RPC); those failures are only logged with `--verbose`.

## Headers carried

| Header | Direction | Notes |
| --- | --- | --- |
| `Mcp-Session-Id` | both | assigned by `serve` at initialize; echoed by `connect` on every later request |
| `MCP-Protocol-Version` | connect → server | value from the initialize result, falling back to `2025-03-26` |
| `Last-Event-ID` | both | SSE resume cursor; `serve` honors it, `connect` sends it |
| `Accept` | connect → server | always `application/json, text/event-stream` on POST |

`portway serve` deliberately does **not** validate `Accept` or
`MCP-Protocol-Version` on POST — a transport adapter should be liberal
in what it accepts, and strictness there breaks plain curl. The one
exception is GET, where a client that cannot parse SSE would only see
garbage, so a wrong `Accept` is refused with `406`.

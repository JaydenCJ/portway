# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2026-07-13

### Added

- `portway serve -- <command>`: expose any stdio MCP server over the
  Streamable HTTP transport on one endpoint (default
  `http://127.0.0.1:8137/mcp`) — POST for requests (answered as
  `application/json`), `202 Accepted` for notifications and responses,
  a GET Server-Sent-Events stream for server-initiated messages, and
  DELETE for session teardown.
- Session lifecycle per the MCP spec: `Mcp-Session-Id` assigned at
  `initialize`, required and validated afterwards (`400` missing,
  `404` stale, i.e. the standard "reinitialize" signal); a new
  `initialize` replaces the session and respawns the child, so a
  restarted client is never locked out.
- Resumable event stream: server-initiated messages carry monotonically
  increasing SSE ids, a bounded replay buffer (`--buffer`, default 256)
  honors `Last-Event-ID`, and duplicate delivery after a racy reconnect
  is impossible by construction.
- `portway connect <url>`: present any Streamable HTTP MCP server as a
  plain stdio server — session id and `MCP-Protocol-Version` headers
  carried automatically, both `application/json` and `text/event-stream`
  POST responses relayed, a background GET listener (with
  `Last-Event-ID` reconnect) for server-initiated messages, a short
  shutdown grace window so a notification sent right after the final
  response still arrives, and `--header` for endpoints that need
  authentication.
- Concurrent request relay in connect mode, so a long-running tool call
  cannot deadlock server-initiated requests such as sampling.
- Failure honesty for stdio clients: connection errors, 4xx/5xx, `202`
  with no response, and SSE streams that end early are all synthesized
  into proper JSON-RPC error responses instead of hanging the client.
- Robust framing on every wire: NDJSON scanner with CRLF tolerance and
  a 32 MiB frame cap, a spec-grammar SSE reader/writer (multi-line
  data, comments, id tracking), and JSON compaction whenever a message
  crosses onto a newline-delimited stream.
- Exact JSON-RPC id routing: numeric and string ids kept distinct,
  large numeric ids preserved textually, duplicate in-flight ids
  refused with `409`.
- `examples/demo-server.sh` + `examples/requests.ndjson`, a
  self-contained shell MCP server for trying both directions offline.
- 92 deterministic offline tests (`go test ./...`, race-clean) and an
  end-to-end `scripts/smoke.sh` that prints `SMOKE OK`.

[0.1.0]: https://github.com/JaydenCJ/portway/releases/tag/v0.1.0

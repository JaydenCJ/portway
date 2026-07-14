# portway examples

This directory contains a tiny fake MCP server and a canned client
session. Everything runs offline with `/bin/sh` and loopback HTTP — no
SDK, no packages, no external network.

## 1. Expose the stdio server over HTTP

`demo-server.sh` answers `initialize`, `tools/list`, `tools/call` and
`ping` on stdio, and emits a server-initiated `notifications/message`
after the first tool call — the message class that can only reach an
HTTP client through the standalone GET event stream.

```bash
cd examples
portway serve --listen 127.0.0.1:8137 -- ./demo-server.sh
```

The bridge prints the endpoint it is serving on stderr. Talk to it with
plain curl (the session id comes back on the initialize response):

```bash
curl -si -X POST http://127.0.0.1:8137/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d @- <<'EOF'
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}
EOF
```

Copy the `Mcp-Session-Id` header into subsequent requests, open
`curl -N -H 'Accept: text/event-stream' -H "Mcp-Session-Id: $SID"
http://127.0.0.1:8137/mcp` in a second terminal to watch server-initiated
messages, and `curl -X DELETE` the endpoint when done.

## 2. Bridge it straight back to stdio

The reverse direction presents any Streamable HTTP endpoint as a plain
stdio server. Chaining both directions is a full round trip and a good
sanity check (this is exactly what `scripts/smoke.sh` automates):

```bash
portway connect http://127.0.0.1:8137/mcp < requests.ndjson
```

Every response — and the server-initiated notification from the GET
stream — comes out as newline-delimited JSON on stdout.

## 3. Use a remote HTTP server from a stdio-only client

Anywhere a client config expects a stdio server command, point it at
`portway connect`:

```json
{
  "mcpServers": {
    "remote-tools": {
      "command": "portway",
      "args": ["connect", "--header", "Authorization: Bearer YOUR_TOKEN", "https://mcp.example.test/mcp"]
    }
  }
}
```

The client speaks stdio as always; portway carries the session id,
protocol-version header and event stream for it.

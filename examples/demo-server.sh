#!/bin/sh
# A tiny fake MCP server speaking newline-delimited JSON-RPC on stdio,
# so you can try both portway directions without installing anything.
# It answers initialize, tools/list, tools/call and ping, and emits a
# server-initiated log notification after the first tools/call — the
# kind of message that must ride the GET event stream over HTTP.
#
# Try it:   portway serve --listen 127.0.0.1:8137 -- ./demo-server.sh

while IFS= read -r line; do
  id=$(printf '%s' "$line" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p')
  case $line in
    *'"initialize"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{},"logging":{}},"serverInfo":{"name":"demo-server","version":"0.3.0"}}}\n' "$id" ;;
    *'"tools/list"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo","description":"Echo text back","inputSchema":{"type":"object","properties":{"text":{"type":"string"}}}}]}}\n' "$id" ;;
    *'"tools/call"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"content":[{"type":"text","text":"hello from stdio"}]}}\n' "$id"
      # Server-initiated notification: over HTTP this can only reach the
      # client through the standalone GET event stream.
      printf '{"jsonrpc":"2.0","method":"notifications/message","params":{"level":"info","data":"echo tool was called"}}\n' ;;
    *'"ping"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{}}\n' "$id" ;;
    *'"notifications/'*)
      : ;; # notifications get no response
    *)
      [ -n "$id" ] && printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"Method not found"}}\n' "$id" ;;
  esac
done

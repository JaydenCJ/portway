#!/usr/bin/env bash
# End-to-end smoke test for portway. No external network (loopback
# only), idempotent, runs from a clean tree. This script plus
# 'go test ./...' is the whole verification story — the repository
# intentionally ships no CI.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKDIR="$(mktemp -d)"
SERVE_PID=""
cleanup() {
  [ -n "$SERVE_PID" ] && kill "$SERVE_PID" 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

fail() {
  echo "SMOKE FAIL: $*" >&2
  exit 1
}

BIN="$WORKDIR/portway"

echo "[1/8] build"
(cd "$ROOT" && go build -o "$BIN" ./cmd/portway) || fail "build failed"

echo "[2/8] --version matches the manifest version"
VERSION_OUT="$("$BIN" --version)"
[ "$VERSION_OUT" = "portway 0.1.0" ] || fail "unexpected version output: $VERSION_OUT"

echo "[3/8] serve: expose the stdio demo server over Streamable HTTP"
cd "$ROOT/examples"
"$BIN" serve --listen 127.0.0.1:0 -- ./demo-server.sh 2> "$WORKDIR/serve.log" &
SERVE_PID=$!
URL=""
for _ in $(seq 1 100); do
  URL="$(sed -n 's|.* at \(http://[^ ]*\)$|\1|p' "$WORKDIR/serve.log" | head -n1)"
  [ -n "$URL" ] && break
  kill -0 "$SERVE_PID" 2>/dev/null || fail "serve exited early: $(cat "$WORKDIR/serve.log")"
  sleep 0.05
done
[ -n "$URL" ] || fail "never saw the listen line in serve stderr"

echo "[4/8] connect: bridge the HTTP endpoint back to stdio (full round trip)"
# Keep stdin open a moment after the last request so the server-initiated
# notification has time to ride the GET stream before shutdown.
{ cat requests.ndjson; sleep 1; } \
  | "$BIN" connect "$URL" > "$WORKDIR/session.out" 2> "$WORKDIR/connect.log" \
  || fail "connect run exited non-zero"
grep -q '"serverInfo":{"name":"demo-server"' "$WORKDIR/session.out" || fail "initialize response missing"
grep -q '"tools":\[{"name":"echo"' "$WORKDIR/session.out" || fail "tools/list response missing"
grep -q '"hello from stdio"' "$WORKDIR/session.out" || fail "tools/call response missing"
grep -q '"id":4,"result":{}' "$WORKDIR/session.out" || fail "ping response missing"

echo "[5/8] server-initiated notification rode the GET event stream"
grep -q '"echo tool was called"' "$WORKDIR/session.out" \
  || fail "notifications/message did not reach stdio"

echo "[6/8] the HTTP side enforces the session contract"
CODE="$(curl -s -o "$WORKDIR/no-session.out" -w '%{http_code}' -X POST "$URL" \
  -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":9,"method":"tools/list"}')"
[ "$CODE" = "400" ] || fail "request without initialize got HTTP $CODE, want 400"
grep -q 'initialize' "$WORKDIR/no-session.out" || fail "400 body is not actionable"

echo "[7/8] a raw curl session works (initialize -> request -> delete)"
curl -s -D "$WORKDIR/init.hdr" -o "$WORKDIR/init.out" -X POST "$URL" \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"0"}}}'
SID="$(tr -d '\r' < "$WORKDIR/init.hdr" | sed -n 's/^[Mm]cp-[Ss]ession-[Ii]d: //p' | head -n1)"
[ -n "$SID" ] || fail "no Mcp-Session-Id header on initialize"
grep -q '"protocolVersion":"2025-06-18"' "$WORKDIR/init.out" || fail "initialize body wrong"
curl -s -X POST "$URL" -H 'Content-Type: application/json' -H "Mcp-Session-Id: $SID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"ping"}' > "$WORKDIR/ping.out"
grep -q '"id":2,"result":{}' "$WORKDIR/ping.out" || fail "ping over curl failed"
DEL="$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$URL" -H "Mcp-Session-Id: $SID")"
[ "$DEL" = "204" ] || fail "DELETE got HTTP $DEL, want 204"

echo "[8/8] connect answers even when nothing is listening"
kill "$SERVE_PID" && wait "$SERVE_PID" 2>/dev/null || true
SERVE_PID=""
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}' \
  | "$BIN" connect "$URL" > "$WORKDIR/refused.out" 2>/dev/null \
  || fail "connect must not crash on a dead endpoint"
grep -q '\-32000' "$WORKDIR/refused.out" || fail "no synthesized error for a dead endpoint"

echo "SMOKE OK"

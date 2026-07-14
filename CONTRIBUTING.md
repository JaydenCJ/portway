# Contributing to portway

Issues, discussions and pull requests are all welcome.

## Getting started

You need Go 1.22 or newer; there are no other dependencies of any kind.

```bash
git clone https://github.com/JaydenCJ/portway.git
cd portway
go build ./...
go test ./...
bash scripts/smoke.sh
```

`scripts/smoke.sh` builds the binary and drives a full serve → connect
round trip against the example stdio server over loopback HTTP —
including the session handshake, the GET event stream, a raw curl
session and failure synthesis; it must finish by printing `SMOKE OK`.

## Before you open a pull request

1. `gofmt -l .` reports nothing (formatting is enforced).
2. `go vet ./...` passes with no findings.
3. `go test ./...` passes (all 92 tests; `-race` should be clean too).
4. `bash scripts/smoke.sh` prints `SMOKE OK`.
5. Add tests for behavior changes; keep logic in pure, unit-testable
   packages (`jsonrpc`, `ndjson`, `sse`, `bus`, `serve`, `connect`)
   rather than in the CLI layer.

## Ground rules

- Zero runtime dependencies is a core feature: the `go.mod` require
  list stays empty. Adding a dependency needs strong justification.
- **portway is a transport adapter, not a gateway.** No auth, no RBAC,
  no policy, no payload rewriting — message bytes pass through
  unchanged (modulo JSON whitespace, which the framings force). PRs
  adding policy features will be redirected to a proxy in front.
- Binds `127.0.0.1` by default; the only outbound traffic is to the
  endpoint the user names on the command line. No telemetry, ever.
- Stay within the MCP Streamable HTTP transport spec. Where the spec
  offers latitude (status codes, single vs multiple GET streams), the
  choice is documented in `docs/transport-mapping.md`; keep it in sync.
- Tests must stay deterministic and offline: `httptest` and loopback
  only, no sleeps, no external services.
- Code comments and doc comments are written in English.

## Reporting bugs

Please include the output of `portway --version`, the exact command
line, the `--verbose` stderr log, and — for protocol issues — a curl
transcript or the stdio frames involved. A repro against
`examples/demo-server.sh` makes any report actionable immediately.

## Security

Please do not open public issues for security problems; use GitHub's
private vulnerability reporting on this repository instead.

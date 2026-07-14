// portway — single-binary bridge that exposes a stdio MCP server over
// Streamable HTTP, and the reverse. A pure transport adapter: no policy,
// no auth, no rewriting — the same JSON-RPC messages, on a different wire.
//
// version:    0.1.0
// author:     JaydenCJ
// license:    MIT
// repository: https://github.com/JaydenCJ/portway
// keywords:   mcp, streamable-http, stdio, json-rpc, bridge, transport, sse
//
// Zero runtime dependencies: the require list below is intentionally empty
// and must stay that way (see CONTRIBUTING.md, "Ground rules").
module github.com/JaydenCJ/portway

go 1.22

// Command portway bridges MCP transports: it exposes a stdio MCP server
// over Streamable HTTP (`portway serve`) or presents a Streamable HTTP
// server as a stdio one (`portway connect`).
package main

import (
	"os"

	"github.com/JaydenCJ/portway/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

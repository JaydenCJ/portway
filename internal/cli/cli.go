// Package cli parses arguments and wires the two bridge directions
// together. All logic that deserves tests lives in the other packages;
// this layer stays thin on purpose.
package cli

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/JaydenCJ/portway/internal/connect"
	"github.com/JaydenCJ/portway/internal/proc"
	"github.com/JaydenCJ/portway/internal/serve"
	"github.com/JaydenCJ/portway/internal/version"
)

const usageText = `portway — bridge between MCP stdio and Streamable HTTP transports

Usage:
  portway serve [flags] -- <command> [args...]   expose a stdio server over HTTP
  portway connect [flags] <url>                  present an HTTP server on stdio
  portway --version                              print the version

serve flags:
  --listen addr    address to bind (default 127.0.0.1:8137; use :0 for a random port)
  --path path      MCP endpoint path (default /mcp)
  --buffer n       server-initiated messages retained for Last-Event-ID replay (default 256)
  --verbose        log one line per notable event to stderr

connect flags:
  --header 'K: V'  extra HTTP header, repeatable (e.g. an Authorization header)
  --no-listen      do not open the GET stream for server-initiated messages
  --verbose        log one line per notable event to stderr

portway is a pure transport adapter: same JSON-RPC messages, different
wire. It binds 127.0.0.1 by default and never talks to anything but the
process or URL you give it.`

// Run is the real main; it returns the process exit code.
func Run(argv []string, stdin io.Reader, stdout, stderr io.Writer) int {
	if len(argv) == 0 {
		fmt.Fprintln(stderr, usageText)
		return 2
	}
	switch argv[0] {
	case "--version", "-v", "version":
		fmt.Fprintf(stdout, "%s %s\n", version.Name, version.Version)
		return 0
	case "--help", "-h", "help":
		fmt.Fprintln(stdout, usageText)
		return 0
	case "serve":
		return runServe(argv[1:], stdout, stderr)
	case "connect":
		return runConnect(argv[1:], stdin, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "portway: unknown command %q\n\n%s\n", argv[0], usageText)
		return 2
	}
}

// serveConfig is the parsed form of `portway serve` arguments.
type serveConfig struct {
	listen  string
	path    string
	buffer  int
	verbose bool
	command []string
}

func parseServe(args []string) (serveConfig, error) {
	cfg := serveConfig{}
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.listen, "listen", "127.0.0.1:8137", "")
	fs.StringVar(&cfg.path, "path", "/mcp", "")
	fs.IntVar(&cfg.buffer, "buffer", 256, "")
	fs.BoolVar(&cfg.verbose, "verbose", false, "")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	cfg.command = fs.Args()
	if len(cfg.command) == 0 {
		return cfg, errors.New("serve needs a server command: portway serve [flags] -- <command> [args...]")
	}
	if !strings.HasPrefix(cfg.path, "/") {
		return cfg, fmt.Errorf("--path must start with '/', got %q", cfg.path)
	}
	if cfg.buffer < 1 {
		return cfg, fmt.Errorf("--buffer must be at least 1, got %d", cfg.buffer)
	}
	if cfg.listen == "" {
		return cfg, errors.New("--listen must not be empty")
	}
	return cfg, nil
}

func runServe(args []string, stdout, stderr io.Writer) int {
	cfg, err := parseServe(args)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, usageText)
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "portway: %v (run 'portway --help' for usage)\n", err)
		return 2
	}
	logf := func(string, ...any) {}
	if cfg.verbose {
		logf = func(format string, a ...any) {
			fmt.Fprintf(stderr, "portway: "+format+"\n", a...)
		}
	}
	factory := func() (serve.Backend, error) {
		logf("starting backend: %s", strings.Join(cfg.command, " "))
		return proc.Start(cfg.command, stderr)
	}
	bridge := serve.NewBridge(factory, serve.Options{
		Path:       cfg.path,
		BufferSize: cfg.buffer,
		Logf:       logf,
	})
	ln, err := net.Listen("tcp", cfg.listen)
	if err != nil {
		fmt.Fprintf(stderr, "portway: %v\n", err)
		return 1
	}
	// This line is load-bearing: scripts (including scripts/smoke.sh)
	// parse it to discover the bound port when --listen uses :0.
	fmt.Fprintf(stderr, "portway %s: serving %q at http://%s%s\n",
		version.Version, strings.Join(cfg.command, " "), ln.Addr(), cfg.path)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	srv := &http.Server{Handler: bridge}
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-ctx.Done()
		bridge.Close() // ends SSE streams and stops the child first
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()
	err = srv.Serve(ln)
	stop()
	<-done
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintf(stderr, "portway: %v\n", err)
		return 1
	}
	fmt.Fprintln(stderr, "portway: shut down")
	return 0
}

// headerFlag collects repeatable --header 'Name: value' flags.
type headerFlag struct {
	h http.Header
}

func (f *headerFlag) String() string { return "" }

func (f *headerFlag) Set(v string) error {
	name, value, ok := strings.Cut(v, ":")
	name = strings.TrimSpace(name)
	value = strings.TrimSpace(value)
	if !ok || name == "" {
		return fmt.Errorf("header must look like 'Name: value', got %q", v)
	}
	if f.h == nil {
		f.h = http.Header{}
	}
	f.h.Add(name, value)
	return nil
}

// connectConfig is the parsed form of `portway connect` arguments.
type connectConfig struct {
	endpoint string
	headers  http.Header
	noListen bool
	verbose  bool
}

func parseConnect(args []string) (connectConfig, error) {
	cfg := connectConfig{}
	var hf headerFlag
	fs := flag.NewFlagSet("connect", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.Var(&hf, "header", "")
	fs.BoolVar(&cfg.noListen, "no-listen", false, "")
	fs.BoolVar(&cfg.verbose, "verbose", false, "")
	if err := fs.Parse(args); err != nil {
		return cfg, err
	}
	rest := fs.Args()
	if len(rest) != 1 {
		return cfg, errors.New("connect needs exactly one endpoint URL: portway connect [flags] <url>")
	}
	u, err := url.Parse(rest[0])
	if err != nil {
		return cfg, fmt.Errorf("invalid endpoint URL: %v", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return cfg, fmt.Errorf("endpoint URL must be http or https, got %q", rest[0])
	}
	if u.Host == "" {
		return cfg, fmt.Errorf("endpoint URL needs a host, got %q", rest[0])
	}
	cfg.endpoint = rest[0]
	cfg.headers = hf.h
	return cfg, nil
}

func runConnect(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	cfg, err := parseConnect(args)
	if errors.Is(err, flag.ErrHelp) {
		fmt.Fprintln(stdout, usageText)
		return 0
	}
	if err != nil {
		fmt.Fprintf(stderr, "portway: %v (run 'portway --help' for usage)\n", err)
		return 2
	}
	opts := connect.Options{
		Endpoint: cfg.endpoint,
		Headers:  cfg.headers,
		NoListen: cfg.noListen,
	}
	if cfg.verbose {
		opts.Logf = func(format string, a ...any) {
			fmt.Fprintf(stderr, "portway: "+format+"\n", a...)
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := connect.Run(ctx, opts, stdin, stdout); err != nil {
		fmt.Fprintf(stderr, "portway: %v\n", err)
		return 1
	}
	return 0
}

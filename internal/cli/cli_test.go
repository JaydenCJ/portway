// Tests for argument parsing and the top-level command dispatch. The
// serving loops themselves are covered by the serve/connect packages and
// scripts/smoke.sh.
package cli

import (
	"strings"
	"testing"
)

func TestVersionOutput(t *testing.T) {
	var out, errOut strings.Builder
	code := Run([]string{"--version"}, strings.NewReader(""), &out, &errOut)
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	if out.String() != "portway 0.1.0\n" {
		t.Fatalf("output = %q", out.String())
	}
}

func TestBadInvocationsShowUsageAndExit2(t *testing.T) {
	var out, errOut strings.Builder
	code := Run(nil, strings.NewReader(""), &out, &errOut)
	if code != 2 {
		t.Fatalf("no args: exit = %d", code)
	}
	if !strings.Contains(errOut.String(), "Usage:") {
		t.Fatalf("stderr = %q", errOut.String())
	}
	errOut.Reset()
	code = Run([]string{"bridge"}, strings.NewReader(""), &out, &errOut)
	if code != 2 || !strings.Contains(errOut.String(), `unknown command "bridge"`) {
		t.Fatalf("unknown command: exit=%d stderr=%q", code, errOut.String())
	}
}

func TestHelpMentionsBothDirections(t *testing.T) {
	var out, errOut strings.Builder
	code := Run([]string{"--help"}, strings.NewReader(""), &out, &errOut)
	if code != 0 {
		t.Fatalf("exit = %d", code)
	}
	for _, want := range []string{"serve", "connect", "stdio", "127.0.0.1"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("help lacks %q", want)
		}
	}
}

func TestSubcommandHelpFlagShowsUsage(t *testing.T) {
	// `portway serve -h` must behave like --help (usage on stdout,
	// exit 0), not surface flag's "help requested" as an error.
	for _, argv := range [][]string{{"serve", "-h"}, {"connect", "--help"}} {
		var out, errOut strings.Builder
		code := Run(argv, strings.NewReader(""), &out, &errOut)
		if code != 0 {
			t.Errorf("%v: exit = %d, want 0 (stderr %q)", argv, code, errOut.String())
		}
		if !strings.Contains(out.String(), "Usage:") {
			t.Errorf("%v: stdout lacks usage: %q", argv, out.String())
		}
	}
}

func TestParseServeDefaults(t *testing.T) {
	cfg, err := parseServe([]string{"--", "my-server", "--flag"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "127.0.0.1:8137" || cfg.path != "/mcp" || cfg.buffer != 256 || cfg.verbose {
		t.Fatalf("cfg = %+v", cfg)
	}
	if len(cfg.command) != 2 || cfg.command[0] != "my-server" || cfg.command[1] != "--flag" {
		t.Fatalf("command = %v", cfg.command)
	}
}

func TestParseServeCustomFlags(t *testing.T) {
	cfg, err := parseServe([]string{
		"--listen", "127.0.0.1:0", "--path", "/bridge", "--buffer", "16", "--verbose",
		"--", "srv"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.listen != "127.0.0.1:0" || cfg.path != "/bridge" || cfg.buffer != 16 || !cfg.verbose {
		t.Fatalf("cfg = %+v", cfg)
	}
}

func TestParseServeCommandFlagsSurviveSeparator(t *testing.T) {
	// Everything after -- belongs to the child, even flag-looking args.
	cfg, err := parseServe([]string{"--", "srv", "--listen", "bogus"})
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.command) != 3 || cfg.command[1] != "--listen" {
		t.Fatalf("command = %v", cfg.command)
	}
	if cfg.listen != "127.0.0.1:8137" {
		t.Fatalf("child flag leaked into portway: %q", cfg.listen)
	}
}

func TestParseServeRejectsBadArgs(t *testing.T) {
	cases := map[string][]string{
		"missing command": {"--listen", ":0"},
		"relative path":   {"--path", "mcp", "--", "srv"},
		"zero buffer":     {"--buffer", "0", "--", "srv"},
	}
	for name, args := range cases {
		if _, err := parseServe(args); err == nil {
			t.Errorf("%s accepted: %v", name, args)
		}
	}
}

func TestParseConnectRejectsBadEndpoints(t *testing.T) {
	cases := map[string][]string{
		"no URL":     nil,
		"two URLs":   {"http://127.0.0.1:1/mcp", "extra"},
		"bad scheme": {"ftp://example.test/mcp"},
		"no scheme":  {"127.0.0.1:8137"},
		"no host":    {"http://"},
	}
	for name, args := range cases {
		if _, err := parseConnect(args); err == nil {
			t.Errorf("%s accepted: %v", name, args)
		}
	}
}

func TestParseConnectAcceptsHTTPAndHTTPS(t *testing.T) {
	for _, u := range []string{"http://127.0.0.1:8137/mcp", "https://example.test/mcp"} {
		cfg, err := parseConnect([]string{u})
		if err != nil {
			t.Errorf("rejected %q: %v", u, err)
			continue
		}
		if cfg.endpoint != u {
			t.Errorf("endpoint = %q", cfg.endpoint)
		}
	}
}

func TestParseConnectHeaders(t *testing.T) {
	cfg, err := parseConnect([]string{
		"--header", "Authorization: Bearer tok",
		"--header", "X-Extra:v",
		"http://127.0.0.1:8137/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.headers.Get("Authorization"); got != "Bearer tok" {
		t.Fatalf("Authorization = %q", got)
	}
	if got := cfg.headers.Get("X-Extra"); got != "v" {
		t.Fatalf("X-Extra = %q", got)
	}
	if _, err := parseConnect([]string{"--header", "no-colon-here", "http://127.0.0.1:1/mcp"}); err == nil {
		t.Fatal("malformed header accepted")
	}
}

func TestParseConnectFlags(t *testing.T) {
	cfg, err := parseConnect([]string{"--no-listen", "--verbose", "http://127.0.0.1:8137/mcp"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.noListen || !cfg.verbose {
		t.Fatalf("cfg = %+v", cfg)
	}
}

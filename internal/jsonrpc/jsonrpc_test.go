// Tests for message classification and id canonicalization — the rules
// every routing decision in both bridge directions depends on.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func mustParse(t *testing.T, in string) Message {
	t.Helper()
	m, err := Parse([]byte(in))
	if err != nil {
		t.Fatalf("Parse(%s) failed: %v", in, err)
	}
	return m
}

func TestParseRequestIDs(t *testing.T) {
	m := mustParse(t, `{"jsonrpc":"2.0","id":7,"method":"tools/list"}`)
	if m.Kind != KindRequest || m.Method != "tools/list" || m.ID != "n:7" || !m.HasID {
		t.Fatalf("number id: %+v", m)
	}
	m = mustParse(t, `{"jsonrpc":"2.0","id":"abc","method":"ping"}`)
	if m.Kind != KindRequest || m.ID != "s:abc" {
		t.Fatalf("string id: kind=%v id=%q", m.Kind, m.ID)
	}
}

func TestIDKeySemantics(t *testing.T) {
	// JSON-RPC ids 1 and "1" are different ids; conflating them would
	// cross-wire two in-flight requests.
	n := mustParse(t, `{"jsonrpc":"2.0","id":1,"method":"a"}`)
	s := mustParse(t, `{"jsonrpc":"2.0","id":"1","method":"a"}`)
	if n.ID == s.ID {
		t.Fatalf("number and string ids collide: %q", n.ID)
	}
	// A float64 round-trip would corrupt ids beyond 2^53.
	big := mustParse(t, `{"jsonrpc":"2.0","id":9007199254740993,"method":"a"}`)
	if big.ID != "n:9007199254740993" {
		t.Fatalf("large id key = %q", big.ID)
	}
	if _, ok := IDKey(nil); ok {
		t.Fatal("absent id reported present")
	}
	if _, ok := IDKey(json.RawMessage("null")); ok {
		t.Fatal("null id reported present")
	}
}

func TestParseNotifications(t *testing.T) {
	m := mustParse(t, `{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	if m.Kind != KindNotification || m.HasID {
		t.Fatalf("got %+v", m)
	}
	// JSON-RPC forbids null request ids; nothing could route an answer,
	// so a method with id null classifies as a notification.
	m = mustParse(t, `{"jsonrpc":"2.0","id":null,"method":"ping"}`)
	if m.Kind != KindNotification {
		t.Fatalf("kind = %v, want notification", m.Kind)
	}
}

func TestParseResultResponse(t *testing.T) {
	m := mustParse(t, `{"jsonrpc":"2.0","id":3,"result":{"ok":true}}`)
	if m.Kind != KindResponse || m.IsError || m.ID != "n:3" {
		t.Fatalf("got %+v", m)
	}
}

func TestParseErrorResponses(t *testing.T) {
	m := mustParse(t, `{"jsonrpc":"2.0","id":3,"error":{"code":-32601,"message":"nope"}}`)
	if m.Kind != KindResponse || !m.IsError {
		t.Fatalf("got %+v", m)
	}
	// Servers answer unparseable requests with id null; still a response.
	m = mustParse(t, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`)
	if m.Kind != KindResponse || m.HasID {
		t.Fatalf("null-id error response: %+v", m)
	}
}

func TestParseBatchRejected(t *testing.T) {
	_, err := Parse([]byte(`[{"jsonrpc":"2.0","id":1,"method":"a"}]`))
	if !errors.Is(err, ErrBatch) {
		t.Fatalf("err = %v, want ErrBatch", err)
	}
}

func TestParseRejectsMalformedInput(t *testing.T) {
	cases := []string{
		``, `   `, `"hi"`, `42`, `true`, // not objects
		`{"jsonrpc":"2.0","id":1,`,              // truncated JSON
		`{"id":1,"method":"a"}`,                 // missing version
		`{"jsonrpc":"1.0","id":1,"method":"a"}`, // wrong version
		`{"jsonrpc":"2.0","id":1}`,              // neither method nor result/error
	}
	for _, in := range cases {
		if _, err := Parse([]byte(in)); err == nil {
			t.Errorf("Parse(%q) unexpectedly succeeded", in)
		}
	}
}

func TestRawIDRoundTrips(t *testing.T) {
	m := mustParse(t, `{"jsonrpc":"2.0","id":"req-9","method":"a"}`)
	if string(m.RawID) != `"req-9"` {
		t.Fatalf("RawID = %s", m.RawID)
	}
	var v string
	if err := json.Unmarshal(m.RawID, &v); err != nil || v != "req-9" {
		t.Fatalf("RawID does not round-trip: %v %q", err, v)
	}
}

func TestCompactLineFlattensPrettyJSON(t *testing.T) {
	pretty := "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 1,\n  \"method\": \"ping\"\n}"
	out, err := CompactLine([]byte(pretty))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.ContainsRune(out, '\n') {
		t.Fatalf("compacted output still contains newlines: %q", out)
	}
	if string(out) != `{"jsonrpc":"2.0","id":1,"method":"ping"}` {
		t.Fatalf("got %s", out)
	}
}

func TestProtocolVersionFromResult(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-06-18","capabilities":{}}}`
	if got := ProtocolVersionFromResult([]byte(body)); got != "2025-06-18" {
		t.Fatalf("got %q", got)
	}
	for _, in := range []string{`{"jsonrpc":"2.0","id":1,"result":{}}`, `not json`} {
		if got := ProtocolVersionFromResult([]byte(in)); got != "" {
			t.Errorf("ProtocolVersionFromResult(%q) = %q, want empty", in, got)
		}
	}
}

func TestErrorResponseShape(t *testing.T) {
	out := ErrorResponse(json.RawMessage(`42`), -32000, "backend exited")
	m, err := Parse(out)
	if err != nil {
		t.Fatalf("synthesized error does not parse: %v", err)
	}
	if m.Kind != KindResponse || !m.IsError || m.ID != "n:42" {
		t.Fatalf("got %+v", m)
	}
	if !strings.Contains(string(out), "backend exited") {
		t.Fatalf("message lost: %s", out)
	}
	// A nil id must render as JSON null, matching servers' behavior for
	// errors that cannot be attributed to a request.
	out = ErrorResponse(nil, -32700, "parse error")
	if !strings.Contains(string(out), `"id":null`) {
		t.Fatalf("got %s", out)
	}
}

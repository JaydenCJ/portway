// Tests for the SSE writer/reader pair against the WHATWG event-stream
// grammar cases that real MCP servers and proxies produce.
package sse

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestWriteEventFormats(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteEvent(&buf, "3", "", []byte(`{"a":1}`)); err != nil {
		t.Fatal(err)
	}
	if want := "id: 3\ndata: {\"a\":1}\n\n"; buf.String() != want {
		t.Fatalf("got %q, want %q", buf.String(), want)
	}
	// Raw newlines inside a data payload would terminate the event early
	// on the wire; the writer must split them into multiple data lines.
	buf.Reset()
	if err := WriteEvent(&buf, "", "", []byte("line1\nline2")); err != nil {
		t.Fatal(err)
	}
	if want := "data: line1\ndata: line2\n\n"; buf.String() != want {
		t.Fatalf("got %q", buf.String())
	}
	buf.Reset()
	if err := WriteEvent(&buf, "", "message", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), "event: message\n") {
		t.Fatalf("got %q", buf.String())
	}
}

func TestWriteComment(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteComment(&buf, "keep-alive"); err != nil {
		t.Fatal(err)
	}
	if buf.String() != ": keep-alive\n\n" {
		t.Fatalf("got %q", buf.String())
	}
}

func TestReaderSimpleEventAndComments(t *testing.T) {
	r := NewReader(strings.NewReader(": ping\n\ndata: hello\n\n"))
	ev, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Data) != "hello" {
		t.Fatalf("data = %q (comments must be invisible)", ev.Data)
	}
}

func TestReaderJoinsMultipleDataLines(t *testing.T) {
	r := NewReader(strings.NewReader("data: a\ndata: b\n\n"))
	ev, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Data) != "a\nb" {
		t.Fatalf("data = %q", ev.Data)
	}
}

func TestReaderFieldParsing(t *testing.T) {
	// CRLF line endings are legal on the wire.
	r := NewReader(strings.NewReader("data: x\r\n\r\n"))
	ev, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Data) != "x" {
		t.Fatalf("CRLF data = %q", ev.Data)
	}
	// "data:x" (no space) is legal; only one leading space is stripped.
	r = NewReader(strings.NewReader("data:x\ndata:  y\n\n"))
	ev, err = r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Data) != "x\n y" {
		t.Fatalf("colon variants data = %q", ev.Data)
	}
}

func TestReaderTracksEventID(t *testing.T) {
	r := NewReader(strings.NewReader("id: 41\ndata: a\n\ndata: b\n\n"))
	ev1, _ := r.Next()
	ev2, _ := r.Next()
	if ev1.ID != "41" {
		t.Fatalf("ev1.ID = %q", ev1.ID)
	}
	// The id persists across events until replaced, per spec.
	if ev2.ID != "41" || r.LastEventID() != "41" {
		t.Fatalf("id not sticky: ev2.ID=%q last=%q", ev2.ID, r.LastEventID())
	}
}

func TestReaderParsesEventName(t *testing.T) {
	r := NewReader(strings.NewReader("event: message\ndata: m\n\n"))
	ev, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if ev.Name != "message" {
		t.Fatalf("name = %q", ev.Name)
	}
}

func TestReaderSkipsDatalessEvents(t *testing.T) {
	// An event with no data lines is not dispatched, per spec.
	r := NewReader(strings.NewReader("event: noop\n\ndata: yes\n\n"))
	ev, err := r.Next()
	if err != nil {
		t.Fatal(err)
	}
	if string(ev.Data) != "yes" || ev.Name != "" {
		t.Fatalf("dataless event dispatched: %+v", ev)
	}
}

func TestReaderDiscardsIncompleteEventAtEOF(t *testing.T) {
	// A stream that dies mid-event must not deliver a half message.
	r := NewReader(strings.NewReader("data: partial"))
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestWriterReaderRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`)
	if err := WriteEvent(&buf, "7", "", payload); err != nil {
		t.Fatal(err)
	}
	ev, err := NewReader(&buf).Next()
	if err != nil {
		t.Fatal(err)
	}
	if ev.ID != "7" || !bytes.Equal(ev.Data, payload) {
		t.Fatalf("round trip lost data: %+v", ev)
	}
}

func TestContentTypeHelpers(t *testing.T) {
	acceptCases := map[string]bool{
		"":                                    true, // lenient: plain curl works
		"text/event-stream":                   true,
		"application/json, text/event-stream": true,
		"text/event-stream; charset=utf-8":    true,
		"*/*":                                 true,
		"TEXT/EVENT-STREAM":                   true,
		"application/json":                    false,
		"text/html,application/xhtml+xml":     false,
	}
	for accept, want := range acceptCases {
		if got := AcceptsEventStream(accept); got != want {
			t.Errorf("AcceptsEventStream(%q) = %v, want %v", accept, got, want)
		}
	}
	if !IsEventStream("text/event-stream; charset=utf-8") {
		t.Fatal("parameterized content type rejected")
	}
	if IsEventStream("application/json") {
		t.Fatal("json accepted as event stream")
	}
}

// Package sse implements the subset of Server-Sent Events used by the
// MCP Streamable HTTP transport: an event writer for the server side and
// an incremental event reader for the client side. Both follow the
// WHATWG event-stream grammar (field parsing, comments, CRLF tolerance,
// multi-line data joined with newlines).
package sse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Event is one dispatched server-sent event.
type Event struct {
	// ID is the last id field seen on or before this event ("" if none).
	ID string
	// Name is the event type ("" means the default "message" type).
	Name string
	// Data is the event payload; multiple data lines are joined with \n.
	Data []byte
}

// WriteEvent serializes one event to w. Data containing newlines is
// split across multiple data: lines, per the SSE grammar. If w is an
// http.ResponseWriter that supports flushing, the event is flushed so it
// reaches the client immediately.
func WriteEvent(w io.Writer, id, name string, data []byte) error {
	var buf bytes.Buffer
	if id != "" {
		fmt.Fprintf(&buf, "id: %s\n", id)
	}
	if name != "" {
		fmt.Fprintf(&buf, "event: %s\n", name)
	}
	for _, line := range bytes.Split(data, []byte("\n")) {
		buf.WriteString("data: ")
		buf.Write(line)
		buf.WriteByte('\n')
	}
	buf.WriteByte('\n')
	if _, err := w.Write(buf.Bytes()); err != nil {
		return err
	}
	flush(w)
	return nil
}

// WriteComment writes a comment line (": text"), used as a keep-alive
// that event-stream parsers must ignore.
func WriteComment(w io.Writer, text string) error {
	if _, err := fmt.Fprintf(w, ": %s\n\n", text); err != nil {
		return err
	}
	flush(w)
	return nil
}

func flush(w io.Writer) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// Reader incrementally parses an event stream. It is not safe for
// concurrent use.
type Reader struct {
	r      *bufio.Reader
	lastID string
}

// NewReader wraps an event-stream body.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: bufio.NewReaderSize(r, 64<<10)}
}

// LastEventID reports the most recent id field seen, for resuming a
// dropped stream via the Last-Event-ID request header.
func (r *Reader) LastEventID() string { return r.lastID }

// Next returns the next dispatched event. Events with no data lines are
// not dispatched (per spec), and an incomplete event at EOF is discarded.
func (r *Reader) Next() (Event, error) {
	var (
		dataLines [][]byte
		name      string
	)
	for {
		line, err := r.readLine()
		if err != nil {
			return Event{}, err
		}
		if len(line) == 0 { // blank line: dispatch if we have data
			if len(dataLines) == 0 {
				name = ""
				continue
			}
			return Event{
				ID:   r.lastID,
				Name: name,
				Data: bytes.Join(dataLines, []byte("\n")),
			}, nil
		}
		if line[0] == ':' {
			continue // comment
		}
		field, value := splitField(line)
		switch field {
		case "data":
			dataLines = append(dataLines, value)
		case "event":
			name = string(value)
		case "id":
			// Ids containing NUL must be ignored, per spec.
			if !bytes.ContainsRune(value, 0) {
				r.lastID = string(value)
			}
		case "retry":
			// Reconnection delay hints are ignored; portway manages its
			// own retry policy.
		default:
			// Unknown fields are ignored, per spec.
		}
	}
}

// readLine returns one line with the trailing LF/CRLF removed. Lines
// longer than the buffer are accumulated. io.EOF discards any partial
// unterminated line, matching browser EventSource behavior.
func (r *Reader) readLine() ([]byte, error) {
	var line []byte
	for {
		chunk, err := r.r.ReadSlice('\n')
		line = append(line, chunk...)
		if err == nil {
			break
		}
		if err == bufio.ErrBufferFull {
			continue
		}
		return nil, err // includes io.EOF: incomplete final line discarded
	}
	line = bytes.TrimSuffix(line, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	return line, nil
}

// splitField parses "field: value" per the SSE grammar: the value starts
// after the first colon, with a single leading space stripped.
func splitField(line []byte) (string, []byte) {
	i := bytes.IndexByte(line, ':')
	if i < 0 {
		return string(line), nil // field with empty value
	}
	field := string(line[:i])
	value := line[i+1:]
	if len(value) > 0 && value[0] == ' ' {
		value = value[1:]
	}
	return field, value
}

// AcceptsEventStream reports whether an Accept header allows
// text/event-stream. An absent header is treated as permissive, so plain
// curl remains usable against the bridge.
func AcceptsEventStream(accept string) bool {
	if strings.TrimSpace(accept) == "" {
		return true
	}
	for _, part := range strings.Split(accept, ",") {
		mt := strings.TrimSpace(strings.SplitN(part, ";", 2)[0])
		switch strings.ToLower(mt) {
		case "text/event-stream", "text/*", "*/*":
			return true
		}
	}
	return false
}

// IsEventStream reports whether a response Content-Type is
// text/event-stream (parameters ignored).
func IsEventStream(contentType string) bool {
	mt := strings.TrimSpace(strings.SplitN(contentType, ";", 2)[0])
	return strings.EqualFold(mt, "text/event-stream")
}

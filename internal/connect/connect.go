// Package connect implements the HTTP→stdio direction: portway presents
// itself as a plain stdio MCP server on stdin/stdout while relaying
// every message to a remote Streamable HTTP endpoint.
//
// Requests are POSTed and their responses — whether the server answers
// with application/json or a text/event-stream — are written back to
// stdout. The Mcp-Session-Id assigned at initialize is carried on every
// later request, and a background GET stream (resumable via
// Last-Event-ID) delivers server-initiated messages. Requests after
// initialize are relayed concurrently, so a server-initiated request
// (e.g. sampling) can be answered while a long tool call is in flight.
package connect

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/JaydenCJ/portway/internal/jsonrpc"
	"github.com/JaydenCJ/portway/internal/ndjson"
	"github.com/JaydenCJ/portway/internal/sse"
)

// Header names used by the MCP Streamable HTTP transport.
const (
	HeaderSessionID       = "Mcp-Session-Id"
	HeaderProtocolVersion = "MCP-Protocol-Version"
	HeaderLastEventID     = "Last-Event-ID"
)

// FallbackProtocolVersion is sent when the initialize result did not
// state one, per the transport's backwards-compatibility rule.
const FallbackProtocolVersion = "2025-03-26"

// maxErrorExcerpt bounds how much of an HTTP error body is quoted in a
// synthesized JSON-RPC error.
const maxErrorExcerpt = 512

// Options configures a connect run.
type Options struct {
	// Endpoint is the MCP endpoint URL, e.g. http://127.0.0.1:8137/mcp.
	Endpoint string
	// Headers are extra headers added to every HTTP request (e.g. an
	// Authorization header the remote requires).
	Headers http.Header
	// Client is the HTTP client to use; nil means a default client with
	// no global timeout (tool calls may legitimately run long).
	Client *http.Client
	// NoListen disables the background GET stream for server-initiated
	// messages.
	NoListen bool
	// RetryDelay is the pause between reconnect attempts of the GET
	// stream. Zero means the 500ms default; tests set it very low.
	RetryDelay time.Duration
	// Linger is how long the GET event stream is kept open after stdin
	// ends, so a server-initiated message sent just after the final
	// response (a log notification, say) is not cut off mid-flight.
	// Zero means the 200ms default; negative disables the grace window.
	Linger time.Duration
	// Logf, when non-nil, receives one line per notable event.
	Logf func(format string, args ...any)
}

// Run bridges stdin/stdout to the remote endpoint until stdin ends.
func Run(ctx context.Context, opts Options, stdin io.Reader, stdout io.Writer) error {
	if opts.Client == nil {
		opts.Client = &http.Client{}
	}
	if opts.RetryDelay == 0 {
		opts.RetryDelay = 500 * time.Millisecond
	}
	if opts.Linger == 0 {
		opts.Linger = 200 * time.Millisecond
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c := &client{opts: opts, ctx: ctx, out: &syncWriter{w: stdout}}

	sc := ndjson.NewScanner(stdin)
	for {
		frame, err := sc.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			c.wg.Wait()
			cancel()
			c.listenWg.Wait()
			c.deleteSession()
			return err
		}
		msg, perr := jsonrpc.Parse(frame)
		if perr != nil {
			// Answer malformed client input the way a stdio server would.
			c.out.writeLine(jsonrpc.ErrorResponse(nil, -32700, "parse error: "+perr.Error()))
			c.logf("dropped malformed stdin frame: %v", perr)
			continue
		}
		if msg.Kind == jsonrpc.KindRequest && msg.Method != "initialize" && c.initialized() {
			// Concurrent relay after the handshake: a blocked tool call
			// must not stop the client from answering server-initiated
			// requests (which also arrive here, as responses).
			m := msg
			c.wg.Add(1)
			go func() {
				defer c.wg.Done()
				c.post(m)
			}()
			continue
		}
		c.post(msg)
	}
	c.wg.Wait() // let in-flight request relays finish
	c.linger()  // grace for server-initiated messages still in flight
	cancel()    // then stop the GET listener
	c.listenWg.Wait()
	c.deleteSession()
	return nil
}

// client is the mutable state of one connect run.
type client struct {
	opts     Options
	ctx      context.Context
	out      *syncWriter
	wg       sync.WaitGroup // in-flight request relays
	listenWg sync.WaitGroup // the background GET listener

	mu        sync.Mutex
	sessionID string
	protocol  string
	init      bool
	listening bool

	idMu        sync.Mutex
	lastEventID string
}

func (c *client) logf(format string, args ...any) {
	if c.opts.Logf != nil {
		c.opts.Logf(format, args...)
	}
}

func (c *client) initialized() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.init
}

// newRequest builds an HTTP request with the transport's standing
// headers: caller-supplied extras, the session id, and the negotiated
// protocol version.
func (c *client) newRequest(method string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(c.ctx, method, c.opts.Endpoint, body)
	if err != nil {
		return nil, err
	}
	for k, vs := range c.opts.Headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	c.mu.Lock()
	if c.sessionID != "" {
		req.Header.Set(HeaderSessionID, c.sessionID)
	}
	if c.protocol != "" {
		req.Header.Set(HeaderProtocolVersion, c.protocol)
	}
	c.mu.Unlock()
	return req, nil
}

// post relays one stdin message to the endpoint and routes whatever
// comes back to stdout.
func (c *client) post(msg jsonrpc.Message) {
	frame, err := jsonrpc.CompactLine(msg.Raw)
	if err != nil {
		c.answerError(msg, "cannot serialize message: "+err.Error())
		return
	}
	req, err := c.newRequest(http.MethodPost, bytes.NewReader(frame))
	if err != nil {
		c.answerError(msg, "cannot build request: "+err.Error())
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	resp, err := c.opts.Client.Do(req)
	if err != nil {
		c.answerError(msg, "POST failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusNoContent:
		if msg.Kind == jsonrpc.KindRequest {
			// A request must produce a response; 202 would hang the client.
			c.answerError(msg, fmt.Sprintf("server accepted the request (HTTP %d) but sent no response", resp.StatusCode))
		}
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		c.relayBody(msg, resp)
	default:
		excerpt := readExcerpt(resp.Body)
		detail := fmt.Sprintf("HTTP %d from %s", resp.StatusCode, c.opts.Endpoint)
		if resp.StatusCode == http.StatusNotFound && c.sessionHeld() {
			detail += " (session expired; the client must reinitialize)"
		}
		if excerpt != "" {
			detail += ": " + excerpt
		}
		if msg.Kind == jsonrpc.KindRequest {
			c.answerError(msg, detail)
		} else {
			c.logf("%s for a %s", detail, msg.Kind)
		}
	}
}

// relayBody writes a successful POST's payload to stdout, handling both
// response content types the transport allows.
func (c *client) relayBody(msg jsonrpc.Message, resp *http.Response) {
	ct, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	switch {
	case sse.IsEventStream(ct):
		// The server chose to stream: related messages first, then the
		// response that closes the stream.
		rd := sse.NewReader(resp.Body)
		for {
			ev, err := rd.Next()
			if err != nil {
				if msg.Kind == jsonrpc.KindRequest {
					c.answerError(msg, "event stream ended before the response arrived")
				}
				return
			}
			m, perr := jsonrpc.Parse(ev.Data)
			if perr != nil {
				c.logf("skipping unparseable SSE event: %v", perr)
				continue
			}
			c.writeMessage(ev.Data)
			if m.Kind == jsonrpc.KindResponse && m.ID == msg.ID && msg.HasID {
				c.finishInitialize(msg, resp, ev.Data)
				return
			}
		}
	default:
		body, err := io.ReadAll(io.LimitReader(resp.Body, ndjson.DefaultMaxFrame))
		if err != nil {
			c.answerError(msg, "reading response body: "+err.Error())
			return
		}
		if len(bytes.TrimSpace(body)) == 0 {
			if msg.Kind == jsonrpc.KindRequest {
				c.answerError(msg, "server returned an empty body for a request")
			}
			return
		}
		c.writeMessage(body)
		c.finishInitialize(msg, resp, body)
	}
}

// finishInitialize captures the session id and protocol version once the
// initialize response has been relayed, then starts the GET listener.
func (c *client) finishInitialize(msg jsonrpc.Message, resp *http.Response, respBody []byte) {
	if msg.Method != "initialize" {
		return
	}
	proto := jsonrpc.ProtocolVersionFromResult(respBody)
	if proto == "" {
		proto = FallbackProtocolVersion
	}
	c.mu.Lock()
	c.sessionID = resp.Header.Get(HeaderSessionID)
	c.protocol = proto
	c.init = true
	startListener := !c.opts.NoListen && !c.listening
	if startListener {
		c.listening = true
	}
	c.mu.Unlock()
	if c.sessionHeld() {
		c.logf("session %s established (protocol %s)", c.sessionID, proto)
	} else {
		c.logf("stateless server (no session id); protocol %s", proto)
	}
	if startListener {
		c.listenWg.Add(1)
		go func() {
			defer c.listenWg.Done()
			c.listen()
		}()
	}
}

// listen keeps one GET event stream open for server-initiated messages,
// resuming with Last-Event-ID after drops, until the run context ends.
func (c *client) listen() {
	first := true
	for {
		if !first {
			select {
			case <-time.After(c.opts.RetryDelay):
			case <-c.ctx.Done():
				return
			}
		}
		first = false
		if c.ctx.Err() != nil {
			return
		}
		req, err := c.newRequest(http.MethodGet, nil)
		if err != nil {
			c.logf("event stream: cannot build request: %v", err)
			return
		}
		req.Header.Set("Accept", "text/event-stream")
		c.idMu.Lock()
		if c.lastEventID != "" {
			req.Header.Set(HeaderLastEventID, c.lastEventID)
		}
		c.idMu.Unlock()
		resp, err := c.opts.Client.Do(req)
		if err != nil {
			if c.ctx.Err() != nil {
				return
			}
			c.logf("event stream: %v (will retry)", err)
			continue
		}
		switch {
		case resp.StatusCode == http.StatusMethodNotAllowed:
			// The server offers no stream at all; that is allowed.
			resp.Body.Close()
			c.logf("server does not offer an event stream (HTTP 405)")
			return
		case resp.StatusCode != http.StatusOK:
			resp.Body.Close()
			c.logf("event stream: HTTP %d (will retry)", resp.StatusCode)
			continue
		case !sse.IsEventStream(resp.Header.Get("Content-Type")):
			resp.Body.Close()
			c.logf("event stream: unexpected content type %q", resp.Header.Get("Content-Type"))
			return
		}
		rd := sse.NewReader(resp.Body)
		for {
			ev, err := rd.Next()
			if err != nil {
				break // stream ended or dropped: reconnect with Last-Event-ID
			}
			if ev.ID != "" {
				c.idMu.Lock()
				c.lastEventID = ev.ID
				c.idMu.Unlock()
			}
			if _, perr := jsonrpc.Parse(ev.Data); perr != nil {
				c.logf("skipping unparseable event-stream message: %v", perr)
				continue
			}
			c.writeMessage(ev.Data)
		}
		resp.Body.Close()
		if c.ctx.Err() != nil {
			return
		}
	}
}

// linger keeps the run alive for the configured grace window after
// stdin EOF when a GET listener is running: a server often emits a
// notification right after answering the last request, and shutting
// down immediately would drop it on the floor.
func (c *client) linger() {
	c.mu.Lock()
	active := c.listening
	c.mu.Unlock()
	if !active || c.opts.Linger < 0 {
		return
	}
	select {
	case <-time.After(c.opts.Linger):
	case <-c.ctx.Done():
	}
}

func (c *client) sessionHeld() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessionID != ""
}

// deleteSession tells the server the session is over, best effort. It
// uses a fresh short-lived context because the run context is already
// canceled by the time stdin has closed.
func (c *client) deleteSession() {
	c.mu.Lock()
	id := c.sessionID
	proto := c.protocol
	c.mu.Unlock()
	if id == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.opts.Endpoint, nil)
	if err != nil {
		return
	}
	for k, vs := range c.opts.Headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set(HeaderSessionID, id)
	if proto != "" {
		req.Header.Set(HeaderProtocolVersion, proto)
	}
	resp, err := c.opts.Client.Do(req)
	if err != nil {
		c.logf("session delete failed: %v", err)
		return
	}
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorExcerpt))
	resp.Body.Close()
	c.logf("session %s deleted", id)
}

// answerError synthesizes the JSON-RPC error a stdio client expects when
// the HTTP leg failed. Notifications get nothing (nothing may answer
// them); the failure is only logged.
func (c *client) answerError(msg jsonrpc.Message, detail string) {
	if msg.Kind != jsonrpc.KindRequest {
		c.logf("%s", detail)
		return
	}
	c.out.writeLine(jsonrpc.ErrorResponse(msg.RawID, -32000, "portway: "+detail))
}

// writeMessage emits one message to stdout as a single NDJSON line. SSE
// data may legally contain embedded newlines; the stdio wire cannot.
func (c *client) writeMessage(data []byte) {
	line, err := jsonrpc.CompactLine(data)
	if err != nil {
		c.logf("dropping non-JSON payload: %v", err)
		return
	}
	c.out.writeLine(line)
}

func readExcerpt(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, maxErrorExcerpt))
	return strings.TrimSpace(string(b))
}

// syncWriter serializes whole-line writes from concurrent relays so
// frames never interleave on stdout.
type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) writeLine(line []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]byte, 0, len(line)+1)
	buf = append(buf, line...)
	buf = append(buf, '\n')
	s.w.Write(buf)
}

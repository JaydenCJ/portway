// Package serve implements the stdio→HTTP direction: an http.Handler
// that speaks the MCP Streamable HTTP transport on one endpoint and
// relays every message to and from a wrapped stdio server, byte-for-byte.
//
// Mapping (see docs/transport-mapping.md for the full contract):
//
//   - POST with a request  → forwarded to the child's stdin; the handler
//     waits for the matching response frame and returns it as
//     application/json.
//   - POST with a notification or response → forwarded; 202 Accepted.
//   - GET → the single SSE stream carrying every server-initiated
//     message (notifications, server→client requests), resumable via
//     Last-Event-ID.
//   - DELETE → terminates the session and stops the child.
//
// One stdio child is one conversation, so the bridge holds exactly one
// session at a time; a new initialize replaces the previous session and
// respawns the child.
package serve

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/JaydenCJ/portway/internal/bus"
	"github.com/JaydenCJ/portway/internal/jsonrpc"
	"github.com/JaydenCJ/portway/internal/sse"
)

// Header names used by the MCP Streamable HTTP transport.
const (
	HeaderSessionID   = "Mcp-Session-Id"
	HeaderLastEventID = "Last-Event-ID"
)

// MaxBodyBytes caps a single POSTed message, mirroring the stdio frame cap.
const MaxBodyBytes = 32 << 20

// heartbeatInterval is how often an idle GET stream emits an SSE comment
// so intermediaries do not time the connection out.
const heartbeatInterval = 15 * time.Second

// Backend is the stdio side of the bridge. *proc.Child implements it;
// tests substitute an in-process fake.
type Backend interface {
	Send(frame []byte) error
	Lines() <-chan []byte
	Done() <-chan struct{}
	Stop()
}

// Factory spawns a fresh Backend for each session.
type Factory func() (Backend, error)

// Options tunes the bridge.
type Options struct {
	// Path is the single MCP endpoint path, e.g. "/mcp".
	Path string
	// BufferSize is how many server-initiated messages are retained for
	// Last-Event-ID replay on the GET stream.
	BufferSize int
	// Logf, when non-nil, receives one line per notable event.
	Logf func(format string, args ...any)
}

// Bridge is the http.Handler exposing one stdio server over HTTP.
type Bridge struct {
	opts    Options
	factory Factory

	initMu sync.Mutex // serializes initialize / replace / close
	mu     sync.Mutex // guards sess
	sess   *session
}

// NewBridge builds a bridge around a backend factory.
func NewBridge(factory Factory, opts Options) *Bridge {
	if opts.Path == "" {
		opts.Path = "/mcp"
	}
	if opts.BufferSize < 1 {
		opts.BufferSize = 256
	}
	return &Bridge{opts: opts, factory: factory}
}

func (b *Bridge) logf(format string, args ...any) {
	if b.opts.Logf != nil {
		b.opts.Logf(format, args...)
	}
}

// Close terminates any active session and stops its child. Safe to call
// multiple times.
func (b *Bridge) Close() {
	b.initMu.Lock()
	defer b.initMu.Unlock()
	b.mu.Lock()
	sess := b.sess
	b.sess = nil
	b.mu.Unlock()
	if sess != nil {
		sess.terminate()
	}
}

func (b *Bridge) current() *session {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sess
}

// ServeHTTP implements the single MCP endpoint.
func (b *Bridge) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != b.opts.Path {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodPost:
		b.handlePost(w, r)
	case http.MethodGet:
		b.handleGet(w, r)
	case http.MethodDelete:
		b.handleDelete(w, r)
	default:
		w.Header().Set("Allow", "POST, GET, DELETE")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// checkSession validates the Mcp-Session-Id header against the active
// session. It writes the error response itself and returns nil when the
// request must not proceed.
func (b *Bridge) checkSession(w http.ResponseWriter, r *http.Request) *session {
	sess := b.current()
	if sess == nil {
		writeRPCError(w, http.StatusBadRequest, nil, -32000,
			"no active session: send an initialize request first")
		return nil
	}
	got := r.Header.Get(HeaderSessionID)
	if got == "" {
		writeRPCError(w, http.StatusBadRequest, nil, -32000,
			"missing "+HeaderSessionID+" header")
		return nil
	}
	if got != sess.id {
		// 404 tells a spec-following client its session expired and it
		// should reinitialize.
		writeRPCError(w, http.StatusNotFound, nil, -32001, "session not found")
		return nil
	}
	return sess
}

func (b *Bridge) handlePost(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
	if err != nil {
		writeRPCError(w, http.StatusRequestEntityTooLarge, nil, -32600, "body too large or unreadable")
		return
	}
	msg, err := jsonrpc.Parse(body)
	if err != nil {
		writeRPCError(w, http.StatusBadRequest, nil, -32700, "parse error: "+err.Error())
		return
	}
	// The child speaks newline-delimited JSON; a pretty-printed HTTP body
	// must be flattened to one line before it touches the stdio wire.
	frame, err := jsonrpc.CompactLine(msg.Raw)
	if err != nil {
		writeRPCError(w, http.StatusBadRequest, nil, -32700, "parse error: "+err.Error())
		return
	}

	if msg.Kind == jsonrpc.KindRequest && msg.Method == "initialize" {
		b.handleInitialize(w, r, msg, frame)
		return
	}

	sess := b.checkSession(w, r)
	if sess == nil {
		return
	}

	switch msg.Kind {
	case jsonrpc.KindNotification, jsonrpc.KindResponse:
		if err := sess.backend.Send(frame); err != nil {
			writeRPCError(w, http.StatusBadGateway, nil, -32000, "backend unavailable: "+err.Error())
			return
		}
		w.WriteHeader(http.StatusAccepted)
	case jsonrpc.KindRequest:
		b.relayRequest(w, r, sess, msg, frame)
	default:
		writeRPCError(w, http.StatusBadRequest, nil, -32600, "invalid request")
	}
}

// relayRequest forwards a client request to the child and waits for the
// matching response frame.
func (b *Bridge) relayRequest(w http.ResponseWriter, r *http.Request, sess *session, msg jsonrpc.Message, frame []byte) {
	ch, err := sess.register(msg.ID)
	if err != nil {
		writeRPCError(w, http.StatusConflict, msg.RawID, -32600,
			"a request with this id is already in flight")
		return
	}
	defer sess.unregister(msg.ID)
	if err := sess.backend.Send(frame); err != nil {
		writeRPCError(w, http.StatusBadGateway, msg.RawID, -32000, "backend unavailable: "+err.Error())
		return
	}
	select {
	case resp := <-ch:
		writeJSON(w, http.StatusOK, resp)
	case <-sess.dead:
		// The child may have answered in the instant before it exited.
		select {
		case resp := <-ch:
			writeJSON(w, http.StatusOK, resp)
		default:
			writeRPCError(w, http.StatusBadGateway, msg.RawID, -32000,
				"backend exited before responding")
		}
	case <-r.Context().Done():
		b.logf("client abandoned %s (id %s)", msg.Method, msg.ID)
	}
}

func (b *Bridge) handleInitialize(w http.ResponseWriter, r *http.Request, msg jsonrpc.Message, frame []byte) {
	b.initMu.Lock()
	defer b.initMu.Unlock()

	// One stdio child is one conversation: a new initialize replaces any
	// previous session so a restarted client is never locked out.
	b.mu.Lock()
	old := b.sess
	b.sess = nil
	b.mu.Unlock()
	if old != nil {
		b.logf("initialize received; replacing session %s", old.id)
		old.terminate()
	}

	backend, err := b.factory()
	if err != nil {
		writeRPCError(w, http.StatusBadGateway, msg.RawID, -32000, "failed to start backend: "+err.Error())
		return
	}
	sess := newSession(newSessionID(), backend, b.opts.BufferSize)
	go sess.dispatch(b.logf)

	ch, _ := sess.register(msg.ID)
	if err := sess.backend.Send(frame); err != nil {
		sess.terminate()
		writeRPCError(w, http.StatusBadGateway, msg.RawID, -32000, "backend unavailable: "+err.Error())
		return
	}
	select {
	case resp := <-ch:
		sess.unregister(msg.ID)
		b.mu.Lock()
		b.sess = sess
		b.mu.Unlock()
		b.logf("session %s established", sess.id)
		w.Header().Set(HeaderSessionID, sess.id)
		writeJSON(w, http.StatusOK, resp)
	case <-sess.dead:
		writeRPCError(w, http.StatusBadGateway, msg.RawID, -32000,
			"backend exited during initialize")
	case <-r.Context().Done():
		sess.terminate()
	}
}

func (b *Bridge) handleGet(w http.ResponseWriter, r *http.Request) {
	if !sse.AcceptsEventStream(r.Header.Get("Accept")) {
		http.Error(w, "the event stream requires Accept: text/event-stream", http.StatusNotAcceptable)
		return
	}
	sess := b.checkSession(w, r)
	if sess == nil {
		return
	}
	var afterID uint64
	if v := r.Header.Get(HeaderLastEventID); v != "" {
		n, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			http.Error(w, "malformed "+HeaderLastEventID, http.StatusBadRequest)
			return
		}
		afterID = n
	}
	replay, ch, cancel, err := sess.bus.Subscribe(afterID)
	if err != nil {
		http.Error(w, "an event stream is already open for this session", http.StatusConflict)
		return
	}
	defer cancel()

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	for _, env := range replay {
		if err := sse.WriteEvent(w, strconv.FormatUint(env.ID, 10), "", env.Data); err != nil {
			return
		}
	}
	heartbeat := time.NewTicker(heartbeatInterval)
	defer heartbeat.Stop()
	for {
		select {
		case env, ok := <-ch:
			if !ok {
				return // session terminated
			}
			if err := sse.WriteEvent(w, strconv.FormatUint(env.ID, 10), "", env.Data); err != nil {
				return
			}
		case <-heartbeat.C:
			if err := sse.WriteComment(w, "keep-alive"); err != nil {
				return
			}
		case <-r.Context().Done():
			return
		}
	}
}

func (b *Bridge) handleDelete(w http.ResponseWriter, r *http.Request) {
	sess := b.checkSession(w, r)
	if sess == nil {
		return
	}
	b.mu.Lock()
	if b.sess == sess {
		b.sess = nil
	}
	b.mu.Unlock()
	sess.terminate()
	b.logf("session %s deleted", sess.id)
	w.WriteHeader(http.StatusNoContent)
}

// session pairs one backend with one HTTP session id.
type session struct {
	id      string
	backend Backend
	bus     *bus.Bus
	dead    chan struct{} // closed when the backend's stdout ends

	mu      sync.Mutex
	pending map[string]chan []byte
}

func newSession(id string, backend Backend, bufferSize int) *session {
	return &session{
		id:      id,
		backend: backend,
		bus:     bus.New(bufferSize),
		dead:    make(chan struct{}),
		pending: make(map[string]chan []byte),
	}
}

// dispatch routes every frame from the child: responses to their waiting
// HTTP handler, everything else (server-initiated requests and
// notifications, and orphan responses) to the GET event stream.
func (s *session) dispatch(logf func(string, ...any)) {
	for frame := range s.backend.Lines() {
		msg, err := jsonrpc.Parse(frame)
		if err == nil && msg.Kind == jsonrpc.KindResponse && msg.HasID {
			if ch := s.take(msg.ID); ch != nil {
				ch <- frame
				continue
			}
		}
		if err != nil && logf != nil {
			logf("unparseable frame from backend passed to event stream: %v", err)
		}
		s.bus.Publish(frame)
	}
	close(s.dead)
	s.bus.Close()
}

func (s *session) register(id string) (chan []byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.pending[id]; exists {
		return nil, fmt.Errorf("duplicate in-flight id %s", id)
	}
	ch := make(chan []byte, 1)
	s.pending[id] = ch
	return ch, nil
}

// take removes and returns the waiter for id, or nil.
func (s *session) take(id string) chan []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := s.pending[id]
	delete(s.pending, id)
	return ch
}

func (s *session) unregister(id string) {
	s.mu.Lock()
	delete(s.pending, id)
	s.mu.Unlock()
}

// terminate stops the backend and waits for dispatch to finish, which
// also closes the event stream.
func (s *session) terminate() {
	s.backend.Stop()
	<-s.dead
}

func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; be defensive.
		panic("portway: cannot read random bytes: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}

func writeRPCError(w http.ResponseWriter, status int, rawID json.RawMessage, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(jsonrpc.ErrorResponse(rawID, code, message))
}

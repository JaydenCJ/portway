// Tests for the stdio-facing side of the bridge, driven against
// httptest servers that emulate real Streamable HTTP behaviors: JSON
// responses, SSE responses, sessions, error statuses and the GET stream.
package connect

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/JaydenCJ/portway/internal/sse"
)

const initFrame = `{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`

const initResult = `{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-06-18","capabilities":{},"serverInfo":{"name":"remote","version":"1.0.0"}}}`

// lockedBuffer collects stdout lines from concurrent relays.
type lockedBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	s := strings.TrimRight(l.b.String(), "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

// run executes a connect session over the given stdin script and
// returns the stdout lines. Extra options are merged over the defaults.
func run(t *testing.T, url, stdin string, mod func(*Options)) []string {
	t.Helper()
	opts := Options{Endpoint: url, RetryDelay: time.Millisecond, Linger: -1}
	if mod != nil {
		mod(&opts)
	}
	var out lockedBuffer
	if err := Run(context.Background(), opts, strings.NewReader(stdin), &out); err != nil {
		t.Fatalf("Run: %v", err)
	}
	return out.Lines()
}

// jsonServer is a minimal Streamable HTTP server for tests: it assigns
// a session id at initialize, answers requests with application/json,
// and 202s everything else. Handlers can be overridden per test.
type jsonServer struct {
	t         *testing.T
	mu        sync.Mutex
	requests  []*http.Request // with bodies pre-read into bodies
	bodies    []string
	sessionID string
	onGet     func(w http.ResponseWriter, r *http.Request)
	onDelete  func(w http.ResponseWriter, r *http.Request)
	onRequest func(w http.ResponseWriter, r *http.Request, body string) bool // true = handled
}

func newJSONServer(t *testing.T) (*jsonServer, *httptest.Server) {
	s := &jsonServer{t: t, sessionID: "sess-1"}
	srv := httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(srv.Close)
	return s, srv
}

func (s *jsonServer) handle(w http.ResponseWriter, r *http.Request) {
	body := ""
	if r.Body != nil {
		var sb strings.Builder
		buf := make([]byte, 4096)
		for {
			n, err := r.Body.Read(buf)
			sb.Write(buf[:n])
			if err != nil {
				break
			}
		}
		body = sb.String()
	}
	s.mu.Lock()
	s.requests = append(s.requests, r.Clone(context.Background()))
	s.bodies = append(s.bodies, body)
	s.mu.Unlock()

	switch r.Method {
	case http.MethodGet:
		if s.onGet != nil {
			s.onGet(w, r)
			return
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	case http.MethodDelete:
		if s.onDelete != nil {
			s.onDelete(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	case http.MethodPost:
		if s.onRequest != nil && s.onRequest(w, r, body) {
			return
		}
		switch {
		case strings.Contains(body, `"initialize"`):
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set(HeaderSessionID, s.sessionID)
			fmt.Fprint(w, initResult)
		case strings.Contains(body, `"id"`) && strings.Contains(body, `"method"`):
			var msg struct {
				ID     json.RawMessage `json:"id"`
				Method string          `json:"method"`
			}
			json.Unmarshal([]byte(body), &msg)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%s,"result":{"echoed":%q}}`, msg.ID, msg.Method)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}
}

func (s *jsonServer) request(i int) (*http.Request, string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requests[i], s.bodies[i]
}

func (s *jsonServer) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.requests)
}

func TestInitializePostHeadersAndRelay(t *testing.T) {
	s, srv := newJSONServer(t)
	lines := run(t, srv.URL, initFrame+"\n", nil)
	if len(lines) < 1 || !strings.Contains(lines[0], `"serverInfo"`) {
		t.Fatalf("stdout = %q", lines)
	}
	req, body := s.request(0)
	if got := req.Header.Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	if got := req.Header.Get("Accept"); got != "application/json, text/event-stream" {
		t.Fatalf("Accept = %q", got)
	}
	if req.Header.Get(HeaderSessionID) != "" {
		t.Fatal("initialize must not carry a session id")
	}
	if !strings.Contains(body, `"clientInfo"`) {
		t.Fatalf("body = %q", body)
	}
}

func TestSessionAndProtocolHeadersAfterInitialize(t *testing.T) {
	s, srv := newJSONServer(t)
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":2,"method":"tools/list"}` + "\n"
	run(t, srv.URL, stdin, nil)
	// requests: 0=initialize POST, then tools/list POST (the DELETE and
	// any GET land in the log too; find the tools/list POST).
	var found bool
	for i := 0; i < s.count(); i++ {
		req, body := s.request(i)
		if req.Method != http.MethodPost || !strings.Contains(body, "tools/list") {
			continue
		}
		found = true
		if got := req.Header.Get(HeaderSessionID); got != "sess-1" {
			t.Fatalf("session header = %q", got)
		}
		if got := req.Header.Get(HeaderProtocolVersion); got != "2025-06-18" {
			t.Fatalf("protocol header = %q", got)
		}
	}
	if !found {
		t.Fatal("tools/list POST never reached the server")
	}
}

func TestJSONResponseWrittenCompact(t *testing.T) {
	_, srv := newJSONServer(t)
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"
	lines := run(t, srv.URL, stdin, nil)
	var respLine string
	for _, l := range lines {
		if strings.Contains(l, `"echoed"`) {
			respLine = l
		}
	}
	if respLine != `{"jsonrpc":"2.0","id":2,"result":{"echoed":"ping"}}` {
		t.Fatalf("response line = %q (all: %q)", respLine, lines)
	}
}

func TestSSEResponseRelaysAllMessages(t *testing.T) {
	// The server streams a progress notification before the response —
	// both must reach stdout, in order, and the response ends the read.
	s, srv := newJSONServer(t)
	s.onRequest = func(w http.ResponseWriter, r *http.Request, body string) bool {
		if !strings.Contains(body, "tools/call") {
			return false
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sse.WriteEvent(w, "", "", []byte(`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":0.5}}`))
		sse.WriteEvent(w, "", "", []byte(`{"jsonrpc":"2.0","id":2,"result":{"content":[]}}`))
		return true
	}
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"echo"}}` + "\n"
	lines := run(t, srv.URL, stdin, nil)
	joined := strings.Join(lines, "\n")
	progressAt := strings.Index(joined, "notifications/progress")
	resultAt := strings.Index(joined, `"content":[]`)
	if progressAt < 0 || resultAt < 0 || progressAt > resultAt {
		t.Fatalf("stream not relayed in order: %q", lines)
	}
}

func TestSSEDataWithNewlinesCompactedToOneLine(t *testing.T) {
	// SSE multi-line data means the JSON arrives with embedded newlines;
	// stdout is NDJSON and must get exactly one line.
	s, srv := newJSONServer(t)
	s.onRequest = func(w http.ResponseWriter, r *http.Request, body string) bool {
		if !strings.Contains(body, `"ping"`) {
			return false
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		pretty := "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 2,\n  \"result\": {}\n}"
		sse.WriteEvent(w, "", "", []byte(pretty))
		return true
	}
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"
	lines := run(t, srv.URL, stdin, nil)
	var got string
	for _, l := range lines {
		if strings.Contains(l, `"id":2`) {
			got = l
		}
	}
	if got != `{"jsonrpc":"2.0","id":2,"result":{}}` {
		t.Fatalf("got %q (all: %q)", got, lines)
	}
}

func TestNotificationGets202AndNoOutput(t *testing.T) {
	_, srv := newJSONServer(t)
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","method":"notifications/initialized"}` + "\n"
	lines := run(t, srv.URL, stdin, nil)
	for _, l := range lines[1:] {
		if strings.Contains(l, "notifications/initialized") {
			t.Fatalf("notification leaked to stdout: %q", lines)
		}
	}
	if len(lines) != 1 { // only the initialize response
		t.Fatalf("stdout = %q", lines)
	}
}

func TestHTTPErrorSynthesizesJSONRPCError(t *testing.T) {
	s, srv := newJSONServer(t)
	s.onRequest = func(w http.ResponseWriter, r *http.Request, body string) bool {
		if !strings.Contains(body, `"boom"`) {
			return false
		}
		http.Error(w, "internal exploded", http.StatusInternalServerError)
		return true
	}
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":9,"method":"boom"}` + "\n"
	lines := run(t, srv.URL, stdin, nil)
	var errLine string
	for _, l := range lines {
		if strings.Contains(l, `"id":9`) {
			errLine = l
		}
	}
	if errLine == "" {
		t.Fatalf("no synthesized error for id 9: %q", lines)
	}
	if !strings.Contains(errLine, `-32000`) || !strings.Contains(errLine, "HTTP 500") ||
		!strings.Contains(errLine, "internal exploded") {
		t.Fatalf("error line = %q", errLine)
	}
}

func TestSessionExpiry404MentionsReinitialize(t *testing.T) {
	s, srv := newJSONServer(t)
	s.onRequest = func(w http.ResponseWriter, r *http.Request, body string) bool {
		if !strings.Contains(body, `"stale"`) {
			return false
		}
		w.WriteHeader(http.StatusNotFound)
		return true
	}
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":5,"method":"stale"}` + "\n"
	lines := run(t, srv.URL, stdin, nil)
	var errLine string
	for _, l := range lines {
		if strings.Contains(l, `"id":5`) {
			errLine = l
		}
	}
	if !strings.Contains(errLine, "reinitialize") {
		t.Fatalf("404 error not actionable: %q", errLine)
	}
}

func TestSwallowedRequestsAlwaysGetAnAnswer(t *testing.T) {
	// Two ways a server can strand a request: 202-ing it, or closing an
	// SSE response stream without ever sending the response. Either way
	// a stdio client would hang forever; portway must answer something.
	s, srv := newJSONServer(t)
	s.onRequest = func(w http.ResponseWriter, r *http.Request, body string) bool {
		switch {
		case strings.Contains(body, `"swallowed"`):
			w.WriteHeader(http.StatusAccepted)
			return true
		case strings.Contains(body, `"truncated"`):
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			sse.WriteEvent(w, "", "", []byte(`{"jsonrpc":"2.0","method":"notifications/progress"}`))
			return true // stream closes with no response
		}
		return false
	}
	stdin := initFrame + "\n" +
		`{"jsonrpc":"2.0","id":6,"method":"swallowed"}` + "\n" +
		`{"jsonrpc":"2.0","id":7,"method":"truncated"}` + "\n"
	lines := run(t, srv.URL, stdin, nil)
	var line6, line7 string
	for _, l := range lines {
		if strings.Contains(l, `"id":6`) {
			line6 = l
		}
		if strings.Contains(l, `"id":7`) {
			line7 = l
		}
	}
	if !strings.Contains(line6, "no response") {
		t.Fatalf("202'd request not answered: %q", lines)
	}
	if !strings.Contains(line7, "ended before") {
		t.Fatalf("truncated stream not surfaced: %q", lines)
	}
}

func TestListenerReceivesServerInitiatedMessages(t *testing.T) {
	s, srv := newJSONServer(t)
	s.onGet = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		sse.WriteEvent(w, "1", "", []byte(`{"jsonrpc":"2.0","method":"notifications/resources/updated","params":{}}`))
		<-r.Context().Done() // hold the stream open until the run ends
	}
	// Hold stdin EOF until the message has actually reached stdout, so
	// shutdown can never race the delivery.
	var out lockedBuffer
	sw := &signalWriter{out: &out, substr: "notifications/resources/updated", ch: make(chan struct{})}
	stdin := &gatedReader{first: initFrame + "\n", gate: sw.ch}
	err := Run(context.Background(),
		Options{Endpoint: srv.URL, RetryDelay: time.Millisecond, Linger: -1},
		stdin, sw)
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(out.Lines(), "\n")
	if !strings.Contains(joined, "notifications/resources/updated") {
		t.Fatalf("server-initiated message lost: %q", joined)
	}
}

// signalWriter forwards writes and closes ch the first time substr has
// appeared in the accumulated output.
type signalWriter struct {
	out    *lockedBuffer
	substr string
	ch     chan struct{}
	mu     sync.Mutex
	fired  bool
	seen   strings.Builder
}

func (s *signalWriter) Write(p []byte) (int, error) {
	n, err := s.out.Write(p)
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.fired {
		s.seen.Write(p)
		if strings.Contains(s.seen.String(), s.substr) {
			s.fired = true
			close(s.ch)
		}
	}
	return n, err
}

// gatedReader yields its first chunk, then holds off EOF until gate
// closes — a deterministic way to keep the run alive while a background
// stream is exercised, with no sleeps.
type gatedReader struct {
	first string
	gate  <-chan struct{}
	pos   int
}

func (g *gatedReader) Read(p []byte) (int, error) {
	if g.pos < len(g.first) {
		n := copy(p, g.first[g.pos:])
		g.pos += n
		return n, nil
	}
	<-g.gate
	return 0, io.EOF
}

func TestListenerReconnectsWithLastEventID(t *testing.T) {
	s, srv := newJSONServer(t)
	reconnected := make(chan struct{})
	var mu sync.Mutex
	var gets int
	var resumeID string
	s.onGet = func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gets++
		n := gets
		if n == 2 {
			resumeID = r.Header.Get(HeaderLastEventID)
		}
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			sse.WriteEvent(w, "41", "", []byte(`{"jsonrpc":"2.0","method":"notifications/message","params":{}}`))
			return // stream drops after one event
		}
		if n == 2 {
			close(reconnected)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		<-r.Context().Done()
	}
	stdin := &gatedReader{first: initFrame + "\n", gate: reconnected}
	var out lockedBuffer
	if err := Run(context.Background(),
		Options{Endpoint: srv.URL, RetryDelay: time.Millisecond, Linger: -1},
		stdin, &out); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if resumeID != "41" {
		t.Fatalf("reconnect Last-Event-ID = %q, want 41", resumeID)
	}
}

func TestServerMessageAfterLastResponseStillDelivered(t *testing.T) {
	// Servers often emit a notification right after answering the last
	// request (a log message, say). stdin EOF races that delivery over
	// the GET stream; the shutdown grace window (Options.Linger) must let
	// it land instead of cutting the stream mid-flight — this is exactly
	// the README quickstart's `connect < requests.ndjson` scenario.
	s, srv := newJSONServer(t)
	answered := make(chan struct{})
	s.onGet = func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		<-answered // only send once the last response is on its way
		sse.WriteEvent(w, "1", "", []byte(`{"jsonrpc":"2.0","method":"notifications/message","params":{"data":"late"}}`))
		<-r.Context().Done()
	}
	s.onRequest = func(w http.ResponseWriter, r *http.Request, body string) bool {
		if !strings.Contains(body, `"ping"`) {
			return false
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":2,"result":{}}`)
		close(answered)
		return true
	}
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"
	lines := run(t, srv.URL, stdin, func(o *Options) { o.Linger = time.Second })
	if !strings.Contains(strings.Join(lines, "\n"), `"data":"late"`) {
		t.Fatalf("late notification lost at shutdown: %q", lines)
	}
}

func TestListenerGivesUpOn405(t *testing.T) {
	// Default jsonServer answers GET with 405: the run must still finish
	// promptly (no retry storm) and relay normally.
	_, srv := newJSONServer(t)
	lines := run(t, srv.URL, initFrame+"\n", nil)
	if len(lines) != 1 {
		t.Fatalf("stdout = %q", lines)
	}
}

func TestDeleteSentOnEOF(t *testing.T) {
	s, srv := newJSONServer(t)
	run(t, srv.URL, initFrame+"\n", func(o *Options) { o.NoListen = true })
	var deleted bool
	for i := 0; i < s.count(); i++ {
		req, _ := s.request(i)
		if req.Method == http.MethodDelete {
			deleted = true
			if got := req.Header.Get(HeaderSessionID); got != "sess-1" {
				t.Fatalf("DELETE session header = %q", got)
			}
		}
	}
	if !deleted {
		t.Fatal("no DELETE sent at shutdown")
	}
}

func TestStatelessServerNoSessionNoDelete(t *testing.T) {
	s, srv := newJSONServer(t)
	s.sessionID = "" // server assigns no session id
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"
	run(t, srv.URL, stdin, func(o *Options) { o.NoListen = true })
	for i := 0; i < s.count(); i++ {
		req, _ := s.request(i)
		if req.Method == http.MethodDelete {
			t.Fatal("DELETE sent to a stateless server")
		}
		if req.Header.Get(HeaderSessionID) != "" {
			t.Fatal("invented a session id the server never assigned")
		}
	}
}

func TestExtraHeadersOnEveryRequest(t *testing.T) {
	s, srv := newJSONServer(t)
	stdin := initFrame + "\n" + `{"jsonrpc":"2.0","id":2,"method":"ping"}` + "\n"
	run(t, srv.URL, stdin, func(o *Options) {
		o.NoListen = true
		o.Headers = http.Header{"Authorization": []string{"Bearer test-token"}}
	})
	if s.count() < 2 {
		t.Fatalf("expected at least 2 requests, got %d", s.count())
	}
	for i := 0; i < s.count(); i++ {
		req, _ := s.request(i)
		if got := req.Header.Get("Authorization"); got != "Bearer test-token" {
			t.Fatalf("request %d (%s) Authorization = %q", i, req.Method, got)
		}
	}
}

func TestMalformedStdinAnsweredWithParseError(t *testing.T) {
	s, srv := newJSONServer(t)
	stdin := "this is not json\n" + initFrame + "\n"
	lines := run(t, srv.URL, stdin, func(o *Options) { o.NoListen = true })
	if len(lines) != 2 {
		t.Fatalf("stdout = %q", lines)
	}
	if !strings.Contains(lines[0], "-32700") {
		t.Fatalf("first line not a parse error: %q", lines[0])
	}
	if !strings.Contains(lines[1], `"serverInfo"`) {
		t.Fatalf("bridge did not recover after junk: %q", lines[1])
	}
	// The junk never reached the wire.
	for i := 0; i < s.count(); i++ {
		_, body := s.request(i)
		if strings.Contains(body, "not json") {
			t.Fatal("malformed frame forwarded to the server")
		}
	}
}

func TestEmptyStdinMakesNoRequests(t *testing.T) {
	s, srv := newJSONServer(t)
	lines := run(t, srv.URL, "", nil)
	if len(lines) != 0 || s.count() != 0 {
		t.Fatalf("lines=%q requests=%d", lines, s.count())
	}
}

func TestConnectionRefusedSynthesizesError(t *testing.T) {
	// A URL nothing listens on: the stdio client still gets an answer.
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close() // now guaranteed refused, still no real network
	var out lockedBuffer
	err := Run(context.Background(),
		Options{Endpoint: url, RetryDelay: time.Millisecond, NoListen: true},
		strings.NewReader(initFrame+"\n"), &out)
	if err != nil {
		t.Fatal(err)
	}
	lines := out.Lines()
	if len(lines) != 1 || !strings.Contains(lines[0], "-32000") {
		t.Fatalf("stdout = %q", lines)
	}
}

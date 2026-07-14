// Tests for the Streamable HTTP side of the bridge, driven through
// httptest against an in-process fake stdio server so every case is
// deterministic and offline.
package serve

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/JaydenCJ/portway/internal/jsonrpc"
	"github.com/JaydenCJ/portway/internal/sse"
)

// fakeBackend is a scriptable in-process stdio server: Send feeds its
// handler, whose returned frames appear on Lines, and Emit injects
// server-initiated messages.
type fakeBackend struct {
	mu      sync.Mutex
	sent    [][]byte
	lines   chan []byte
	done    chan struct{}
	stopped bool
	handler func(msg jsonrpc.Message) [][]byte
}

func newFakeBackend() *fakeBackend {
	f := &fakeBackend{
		lines: make(chan []byte, 64),
		done:  make(chan struct{}),
	}
	f.handler = defaultHandler
	return f
}

// defaultHandler behaves like a minimal MCP server.
func defaultHandler(msg jsonrpc.Message) [][]byte {
	if msg.Kind != jsonrpc.KindRequest {
		return nil
	}
	id := string(msg.RawID)
	switch msg.Method {
	case "initialize":
		return [][]byte{[]byte(fmt.Sprintf(
			`{"jsonrpc":"2.0","id":%s,"result":{"protocolVersion":"2025-06-18","capabilities":{"tools":{}},"serverInfo":{"name":"fake","version":"1.0.0"}}}`, id))}
	case "tools/list":
		return [][]byte{[]byte(fmt.Sprintf(
			`{"jsonrpc":"2.0","id":%s,"result":{"tools":[{"name":"echo"}]}}`, id))}
	default:
		return [][]byte{[]byte(fmt.Sprintf(
			`{"jsonrpc":"2.0","id":%s,"result":{"method":%q}}`, id, msg.Method))}
	}
}

func (f *fakeBackend) Send(frame []byte) error {
	f.mu.Lock()
	if f.stopped {
		f.mu.Unlock()
		return fmt.Errorf("fake backend stopped")
	}
	f.sent = append(f.sent, append([]byte(nil), frame...))
	h := f.handler
	f.mu.Unlock()
	msg, err := jsonrpc.Parse(frame)
	if err != nil {
		return nil // a real child would just ignore junk
	}
	for _, out := range h(msg) {
		f.lines <- out
	}
	return nil
}

func (f *fakeBackend) Emit(frame string) { f.lines <- []byte(frame) }

func (f *fakeBackend) Lines() <-chan []byte  { return f.lines }
func (f *fakeBackend) Done() <-chan struct{} { return f.done }

func (f *fakeBackend) Stop() {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.stopped {
		return
	}
	f.stopped = true
	close(f.lines)
	close(f.done)
}

func (f *fakeBackend) sentFrames() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.sent))
	for i, s := range f.sent {
		out[i] = string(s)
	}
	return out
}

// harness spins up a bridge over fresh fake backends. backends records
// every backend the factory produced.
type harness struct {
	t        *testing.T
	server   *httptest.Server
	bridge   *Bridge
	mu       sync.Mutex
	backends []*fakeBackend
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t}
	h.bridge = NewBridge(func() (Backend, error) {
		f := newFakeBackend()
		h.mu.Lock()
		h.backends = append(h.backends, f)
		h.mu.Unlock()
		return f, nil
	}, Options{Path: "/mcp", BufferSize: 8})
	h.server = httptest.NewServer(h.bridge)
	t.Cleanup(func() {
		h.bridge.Close()
		h.server.Close()
	})
	return h
}

func (h *harness) backend(i int) *fakeBackend {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.backends[i]
}

func (h *harness) url() string { return h.server.URL + "/mcp" }

func (h *harness) post(body string, headers map[string]string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(http.MethodPost, h.url(), strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := h.server.Client().Do(req)
	if err != nil {
		h.t.Fatal(err)
	}
	return resp
}

// initialize performs the handshake and returns the session id.
func (h *harness) initialize() string {
	h.t.Helper()
	resp := h.post(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"0"}}}`, nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		h.t.Fatalf("initialize: HTTP %d", resp.StatusCode)
	}
	sid := resp.Header.Get(HeaderSessionID)
	if sid == "" {
		h.t.Fatal("initialize response carries no session id")
	}
	return sid
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestInitializeEstablishesSession(t *testing.T) {
	h := newHarness(t)
	resp := h.post(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}`, nil)
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, body)
	}
	if resp.Header.Get(HeaderSessionID) == "" {
		t.Fatal("no Mcp-Session-Id header")
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
	if !strings.Contains(body, `"protocolVersion":"2025-06-18"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestRequestBeforeInitializeRejected(t *testing.T) {
	h := newHarness(t)
	resp := h.post(`{"jsonrpc":"2.0","id":1,"method":"tools/list"}`, nil)
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}
	if !strings.Contains(body, "initialize") {
		t.Fatalf("unhelpful error: %s", body)
	}
}

func TestMissingSessionHeaderRejected(t *testing.T) {
	h := newHarness(t)
	h.initialize()
	resp := h.post(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`, nil)
	readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("HTTP %d, want 400", resp.StatusCode)
	}
}

func TestWrongSessionIDIs404(t *testing.T) {
	h := newHarness(t)
	h.initialize()
	resp := h.post(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		map[string]string{HeaderSessionID: "not-the-session"})
	readBody(t, resp)
	// 404 is the spec's "session expired, reinitialize" signal.
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("HTTP %d, want 404", resp.StatusCode)
	}
}

func TestRequestResponseRoundTrip(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	resp := h.post(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`,
		map[string]string{HeaderSessionID: sid})
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"tools":[{"name":"echo"}]`) {
		t.Fatalf("body = %s", body)
	}
}

func TestConcurrentRequestsRouteByID(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	var wg sync.WaitGroup
	results := make([]string, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"m%d"}`, 100+i, i)
			resp := h.post(body, map[string]string{HeaderSessionID: sid})
			results[i] = readBody(t, resp)
		}(i)
	}
	wg.Wait()
	for i, body := range results {
		wantID := fmt.Sprintf(`"id":%d`, 100+i)
		wantMethod := fmt.Sprintf(`"method":"m%d"`, i)
		if !strings.Contains(body, wantID) || !strings.Contains(body, wantMethod) {
			t.Errorf("request %d got someone else's response: %s", i, body)
		}
	}
}

func TestNotificationForwardedWith202(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	resp := h.post(`{"jsonrpc":"2.0","method":"notifications/initialized"}`,
		map[string]string{HeaderSessionID: sid})
	readBody(t, resp)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("HTTP %d, want 202", resp.StatusCode)
	}
	frames := h.backend(0).sentFrames()
	last := frames[len(frames)-1]
	if !strings.Contains(last, "notifications/initialized") {
		t.Fatalf("notification not forwarded; frames = %v", frames)
	}
}

func TestClientResponseForwardedWith202(t *testing.T) {
	// A client answering a server-initiated request POSTs a response.
	h := newHarness(t)
	sid := h.initialize()
	resp := h.post(`{"jsonrpc":"2.0","id":"srv-1","result":{"answer":42}}`,
		map[string]string{HeaderSessionID: sid})
	readBody(t, resp)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("HTTP %d, want 202", resp.StatusCode)
	}
	frames := h.backend(0).sentFrames()
	if !strings.Contains(frames[len(frames)-1], `"answer":42`) {
		t.Fatalf("response not forwarded; frames = %v", frames)
	}
}

func TestMalformedPostsRejectedWithParseError(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	// Batches were removed from the transport in 2025-06-18; portway
	// refuses rather than mistranslating them.
	resp := h.post(`[{"jsonrpc":"2.0","id":9,"method":"ping"}]`,
		map[string]string{HeaderSessionID: sid})
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "-32700") {
		t.Fatalf("batch: HTTP %d: %s", resp.StatusCode, body)
	}
	// Truncated JSON gets the same treatment, before any session check.
	resp = h.post(`{"jsonrpc":`, nil)
	body = readBody(t, resp)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(body, "-32700") {
		t.Fatalf("truncated: HTTP %d: %s", resp.StatusCode, body)
	}
}

func TestPrettyPrintedBodyCompactedForStdio(t *testing.T) {
	// The HTTP side legally accepts pretty JSON; the stdio wire is
	// newline-delimited and must receive exactly one line.
	h := newHarness(t)
	sid := h.initialize()
	pretty := "{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 5,\n  \"method\": \"ping\"\n}"
	resp := h.post(pretty, map[string]string{HeaderSessionID: sid})
	readBody(t, resp)
	frames := h.backend(0).sentFrames()
	last := frames[len(frames)-1]
	if strings.ContainsRune(last, '\n') {
		t.Fatalf("multi-line frame reached the stdio wire: %q", last)
	}
	if last != `{"jsonrpc":"2.0","id":5,"method":"ping"}` {
		t.Fatalf("frame = %q", last)
	}
}

func TestDuplicateInFlightIDRefused(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	// Park one request forever: the handler swallows "slow" and signals
	// once the frame has reached the backend. Because the bridge
	// registers the pending id *before* sending, id 77 is certainly
	// registered by the time the signal fires — no polling, no sleeps.
	received := make(chan struct{})
	h.backend(0).mu.Lock()
	h.backend(0).handler = func(msg jsonrpc.Message) [][]byte {
		if msg.Method == "slow" {
			close(received)
			return nil // never answered
		}
		return defaultHandler(msg)
	}
	h.backend(0).mu.Unlock()

	go func() {
		resp := h.post(`{"jsonrpc":"2.0","id":77,"method":"slow"}`,
			map[string]string{HeaderSessionID: sid})
		resp.Body.Close()
	}()
	<-received
	resp := h.post(`{"jsonrpc":"2.0","id":77,"method":"ping"}`,
		map[string]string{HeaderSessionID: sid})
	readBody(t, resp)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate in-flight id got HTTP %d, want 409", resp.StatusCode)
	}
}

func TestGetStreamsServerInitiatedMessages(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	req, _ := http.NewRequest(http.MethodGet, h.url(), nil)
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set(HeaderSessionID, sid)
	resp, err := h.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}
	if !sse.IsEventStream(resp.Header.Get("Content-Type")) {
		t.Fatalf("content type = %q", resp.Header.Get("Content-Type"))
	}
	h.backend(0).Emit(`{"jsonrpc":"2.0","method":"notifications/progress","params":{"progress":1}}`)
	ev, err := sse.NewReader(resp.Body).Next()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ev.Data), "notifications/progress") {
		t.Fatalf("event data = %s", ev.Data)
	}
	if ev.ID != "1" {
		t.Fatalf("event id = %q, want 1", ev.ID)
	}
}

func TestGetPreconditions(t *testing.T) {
	h := newHarness(t)
	get := func(headers map[string]string) int {
		req, _ := http.NewRequest(http.MethodGet, h.url(), nil)
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, err := h.server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		readBody(t, resp)
		return resp.StatusCode
	}
	// Before any session exists a stream makes no sense.
	if code := get(map[string]string{"Accept": "text/event-stream"}); code != http.StatusBadRequest {
		t.Fatalf("no session: HTTP %d, want 400", code)
	}
	sid := h.initialize()
	// A client that cannot consume SSE must be told so up front.
	if code := get(map[string]string{"Accept": "application/json", HeaderSessionID: sid}); code != http.StatusNotAcceptable {
		t.Fatalf("wrong accept: HTTP %d, want 406", code)
	}
	// A garbage resume cursor is a client bug, not something to guess at.
	if code := get(map[string]string{
		"Accept": "text/event-stream", HeaderSessionID: sid, HeaderLastEventID: "not-a-number",
	}); code != http.StatusBadRequest {
		t.Fatalf("bad Last-Event-ID: HTTP %d, want 400", code)
	}
}

func TestSecondGetStreamRefused(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	open := func() *http.Response {
		req, _ := http.NewRequest(http.MethodGet, h.url(), nil)
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set(HeaderSessionID, sid)
		resp, err := h.server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	first := open()
	defer first.Body.Close()
	second := open()
	readBody(t, second)
	if second.StatusCode != http.StatusConflict {
		t.Fatalf("HTTP %d, want 409", second.StatusCode)
	}
}

func TestLastEventIDReplays(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	// Two server-initiated messages arrive; drain them through a first
	// stream so both are certainly buffered, then resume from event 1.
	h.backend(0).Emit(`{"jsonrpc":"2.0","method":"notifications/message","params":{"n":1}}`)
	h.backend(0).Emit(`{"jsonrpc":"2.0","method":"notifications/message","params":{"n":2}}`)
	firstReq, _ := http.NewRequest(http.MethodGet, h.url(), nil)
	firstReq.Header.Set("Accept", "text/event-stream")
	firstReq.Header.Set(HeaderSessionID, sid)
	first, err := h.server.Client().Do(firstReq)
	if err != nil {
		t.Fatal(err)
	}
	rd := sse.NewReader(first.Body)
	for i := 0; i < 2; i++ {
		if _, err := rd.Next(); err != nil {
			t.Fatalf("draining first stream: %v", err)
		}
	}
	first.Body.Close() // simulate the dropped connection

	// The server may take an instant to notice the drop and free the
	// stream slot; retry on 409 until it does (no sleeps needed — each
	// attempt is a full request round trip).
	var resp *http.Response
	for i := 0; i < 10000; i++ {
		req, _ := http.NewRequest(http.MethodGet, h.url(), nil)
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set(HeaderSessionID, sid)
		req.Header.Set(HeaderLastEventID, "1")
		r, err := h.server.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if r.StatusCode == http.StatusConflict {
			readBody(t, r)
			continue
		}
		resp = r
		break
	}
	if resp == nil {
		t.Fatal("stream slot never freed after disconnect")
	}
	defer resp.Body.Close()
	ev, err := sse.NewReader(resp.Body).Next()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ev.Data), `"n":2`) || ev.ID != "2" {
		t.Fatalf("resume delivered wrong event: id=%q data=%s", ev.ID, ev.Data)
	}
}

func TestDeleteTerminatesSession(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	req, _ := http.NewRequest(http.MethodDelete, h.url(), nil)
	req.Header.Set(HeaderSessionID, sid)
	resp, err := h.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	readBody(t, resp)
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("HTTP %d, want 204", resp.StatusCode)
	}
	select {
	case <-h.backend(0).Done():
	default:
		t.Fatal("backend not stopped by DELETE")
	}
	// The session is gone: further requests must be rejected.
	after := h.post(`{"jsonrpc":"2.0","id":9,"method":"ping"}`,
		map[string]string{HeaderSessionID: sid})
	readBody(t, after)
	if after.StatusCode != http.StatusBadRequest {
		t.Fatalf("HTTP %d after delete", after.StatusCode)
	}
}

func TestReinitializeReplacesSession(t *testing.T) {
	h := newHarness(t)
	first := h.initialize()
	second := h.initialize()
	if first == second {
		t.Fatal("session id was reused")
	}
	select {
	case <-h.backend(0).Done():
	default:
		t.Fatal("old backend still running after reinitialize")
	}
	// The new session works; the old id does not.
	ok := h.post(`{"jsonrpc":"2.0","id":2,"method":"ping"}`,
		map[string]string{HeaderSessionID: second})
	readBody(t, ok)
	if ok.StatusCode != http.StatusOK {
		t.Fatalf("new session broken: HTTP %d", ok.StatusCode)
	}
	stale := h.post(`{"jsonrpc":"2.0","id":3,"method":"ping"}`,
		map[string]string{HeaderSessionID: first})
	readBody(t, stale)
	if stale.StatusCode != http.StatusNotFound {
		t.Fatalf("stale session: HTTP %d, want 404", stale.StatusCode)
	}
}

func TestBackendExitDuringRequestIs502(t *testing.T) {
	h := newHarness(t)
	sid := h.initialize()
	h.backend(0).mu.Lock()
	h.backend(0).handler = func(msg jsonrpc.Message) [][]byte {
		if msg.Method == "die" {
			go h.backend(0).Stop() // exit without answering
			return nil
		}
		return defaultHandler(msg)
	}
	h.backend(0).mu.Unlock()
	resp := h.post(`{"jsonrpc":"2.0","id":13,"method":"die"}`,
		map[string]string{HeaderSessionID: sid})
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, body)
	}
	var env struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
		ID json.RawMessage `json:"id"`
	}
	if err := json.Unmarshal([]byte(body), &env); err != nil {
		t.Fatalf("502 body is not JSON-RPC: %s", body)
	}
	if !bytes.Equal(env.ID, []byte("13")) {
		t.Fatalf("error echoes wrong id: %s", env.ID)
	}
}

func TestUnknownMethodAndPath(t *testing.T) {
	h := newHarness(t)
	req, _ := http.NewRequest(http.MethodPut, h.url(), strings.NewReader("{}"))
	resp, err := h.server.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	readBody(t, resp)
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("HTTP %d", resp.StatusCode)
	}
	if allow := resp.Header.Get("Allow"); !strings.Contains(allow, "POST") {
		t.Fatalf("Allow = %q", allow)
	}
	// The bridge serves exactly one endpoint path.
	resp, err = h.server.Client().Post(h.server.URL+"/other", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
	if err != nil {
		t.Fatal(err)
	}
	readBody(t, resp)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("wrong path: HTTP %d", resp.StatusCode)
	}
}

func TestFactoryFailureIs502(t *testing.T) {
	b := NewBridge(func() (Backend, error) {
		return nil, fmt.Errorf("spawn refused")
	}, Options{Path: "/mcp"})
	srv := httptest.NewServer(b)
	defer srv.Close()
	resp, err := srv.Client().Post(srv.URL+"/mcp", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
	if err != nil {
		t.Fatal(err)
	}
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(body, "spawn refused") {
		t.Fatalf("HTTP %d: %s", resp.StatusCode, body)
	}
}

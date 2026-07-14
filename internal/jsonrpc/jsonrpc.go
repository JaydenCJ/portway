// Package jsonrpc classifies JSON-RPC 2.0 messages just enough to route
// them between transports: request vs notification vs response, plus a
// canonical id key for pairing. It never rewrites payloads — portway is
// a transport adapter, and the message body is not its business.
package jsonrpc

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
)

// Kind is the JSON-RPC message class relevant to routing.
type Kind int

const (
	// KindInvalid marks input that is not a well-formed JSON-RPC 2.0 message.
	KindInvalid Kind = iota
	// KindRequest has a method and an id and expects exactly one response.
	KindRequest
	// KindNotification has a method but no id; nothing may answer it.
	KindNotification
	// KindResponse carries a result or an error for a previous request.
	KindResponse
)

// String returns a short lowercase label used in logs.
func (k Kind) String() string {
	switch k {
	case KindRequest:
		return "request"
	case KindNotification:
		return "notification"
	case KindResponse:
		return "response"
	default:
		return "invalid"
	}
}

// Message is the routing-relevant view of one JSON-RPC message. Raw is
// the original bytes, untouched; everything else is derived.
type Message struct {
	Raw    []byte
	Kind   Kind
	Method string
	// ID is a canonical key for the message id: "n:<number>" for numeric
	// ids, "s:<string>" for string ids. Empty when the id is absent or
	// null. Numeric 1 and string "1" deliberately map to different keys,
	// as JSON-RPC treats them as distinct ids.
	ID    string
	HasID bool
	// RawID is the id member exactly as it appeared, for echoing back in
	// synthesized error responses. Nil when absent.
	RawID json.RawMessage
	// IsError is true for responses that carry an error member.
	IsError bool
}

// ErrBatch is returned for JSON arrays: JSON-RPC batching was removed
// from the MCP Streamable HTTP transport in protocol version 2025-06-18,
// and portway does not translate it.
var ErrBatch = errors.New("jsonrpc: batch arrays are not supported")

type envelope struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Result  json.RawMessage `json:"result"`
	Error   json.RawMessage `json:"error"`
}

// Parse classifies a single JSON-RPC 2.0 message. The returned Message
// keeps a reference to data; callers that reuse buffers must copy first.
func Parse(data []byte) (Message, error) {
	m := Message{Raw: data, Kind: KindInvalid}
	trim := bytes.TrimLeft(data, " \t\r\n")
	if len(trim) == 0 {
		return m, errors.New("jsonrpc: empty input")
	}
	if trim[0] == '[' {
		return m, ErrBatch
	}
	if trim[0] != '{' {
		return m, fmt.Errorf("jsonrpc: not a JSON object (starts with %q)", trim[0])
	}
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return m, fmt.Errorf("jsonrpc: invalid JSON: %w", err)
	}
	if env.JSONRPC != "2.0" {
		return m, fmt.Errorf("jsonrpc: missing or wrong jsonrpc version %q", env.JSONRPC)
	}
	m.Method = env.Method
	m.ID, m.HasID = IDKey(env.ID)
	if m.HasID {
		m.RawID = append(json.RawMessage(nil), bytes.TrimSpace(env.ID)...)
	}
	switch {
	case env.Method != "" && m.HasID:
		m.Kind = KindRequest
	case env.Method != "":
		// A method with a null id is treated as a notification: JSON-RPC
		// forbids null request ids, and nothing could be routed back.
		m.Kind = KindNotification
	case env.Result != nil || env.Error != nil:
		// Error responses may carry id null (the request was unparseable);
		// they classify as responses with HasID == false.
		m.Kind = KindResponse
		m.IsError = env.Error != nil
	default:
		return m, errors.New("jsonrpc: neither method nor result/error present")
	}
	return m, nil
}

// IDKey canonicalizes a raw JSON-RPC id into a map key. Numbers keep
// their exact JSON text (no float round-trip), so large ids survive.
func IDKey(raw json.RawMessage) (string, bool) {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return "", false
	}
	if raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return "", false
		}
		return "s:" + s, true
	}
	// Validate it is a JSON number; keep the source text as the key.
	var n json.Number
	if err := json.Unmarshal(raw, &n); err != nil {
		return "", false
	}
	return "n:" + string(raw), true
}

// CompactLine renders a JSON document on a single line with no interior
// newlines, suitable for a newline-delimited stdio transport. HTTP
// bodies and SSE data may legally arrive pretty-printed; a stdio wire
// cannot carry them that way.
func CompactLine(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	if err := json.Compact(&buf, data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ProtocolVersionFromResult extracts result.protocolVersion from an
// initialize response, or "" when absent. Used to populate the
// MCP-Protocol-Version header on subsequent HTTP requests.
func ProtocolVersionFromResult(data []byte) string {
	var env struct {
		Result struct {
			ProtocolVersion string `json:"protocolVersion"`
		} `json:"result"`
	}
	if err := json.Unmarshal(data, &env); err != nil {
		return ""
	}
	return env.Result.ProtocolVersion
}

// ErrorResponse builds a minimal JSON-RPC error response. rawID is the
// original id JSON (nil renders as null). Used only for errors portway
// itself must synthesize (backend gone, HTTP failure); real responses
// always pass through untouched.
func ErrorResponse(rawID json.RawMessage, code int, message string) []byte {
	if len(rawID) == 0 {
		rawID = json.RawMessage("null")
	}
	body := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}{JSONRPC: "2.0", ID: rawID}
	body.Error.Code = code
	body.Error.Message = message
	out, err := json.Marshal(body)
	if err != nil {
		// Marshal of this shape cannot fail; keep a defensive fallback.
		return []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal error"}}`)
	}
	return out
}

// Package bus is the ordered, replayable buffer behind the bridge's GET
// event stream. Server-initiated messages are appended with monotonically
// increasing event ids; a (single) subscriber receives live messages and
// can resume from a Last-Event-ID after a dropped connection.
package bus

import (
	"errors"
	"sync"
)

// Envelope is one buffered message with its SSE event id.
type Envelope struct {
	ID   uint64
	Data []byte
}

// ErrBusy is returned when a subscriber is already attached. The MCP
// Streamable HTTP spec lets a server refuse extra GET streams; portway
// keeps exactly one so no message is ever split across listeners.
var ErrBusy = errors.New("bus: a stream is already attached")

// Bus retains the last `limit` messages for replay and feeds at most one
// live subscriber. All methods are safe for concurrent use.
type Bus struct {
	mu       sync.Mutex
	next     uint64
	buf      []Envelope
	limit    int
	sub      chan Envelope
	subAfter uint64
	closed   bool
}

// New creates a Bus retaining up to limit messages (minimum 1).
func New(limit int) *Bus {
	if limit < 1 {
		limit = 1
	}
	return &Bus{next: 1, limit: limit}
}

// Publish appends data and returns its event id (0 after Close). If a
// subscriber is attached but not draining, the live send is dropped —
// the message stays in the replay buffer, so a reconnect with
// Last-Event-ID recovers it.
func (b *Bus) Publish(data []byte) uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return 0
	}
	env := Envelope{ID: b.next, Data: data}
	b.next++
	b.buf = append(b.buf, env)
	if len(b.buf) > b.limit {
		b.buf = b.buf[len(b.buf)-b.limit:]
	}
	// Never hand the live subscriber an event it declared already seen
	// via Last-Event-ID, even if the publish raced its subscription.
	if b.sub != nil && env.ID > b.subAfter {
		select {
		case b.sub <- env:
		default: // slow subscriber: rely on replay after reconnect
		}
	}
	return env.ID
}

// Subscribe attaches the single live subscriber. Messages with id >
// afterID that are still buffered are returned for replay; later
// messages arrive on the channel. cancel detaches (idempotent). The
// channel is closed by Close.
func (b *Bus) Subscribe(afterID uint64) (replay []Envelope, ch <-chan Envelope, cancel func(), err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		closed := make(chan Envelope)
		close(closed)
		return nil, closed, func() {}, nil
	}
	if b.sub != nil {
		return nil, nil, nil, ErrBusy
	}
	for _, env := range b.buf {
		if env.ID > afterID {
			replay = append(replay, env)
		}
	}
	sub := make(chan Envelope, b.limit)
	b.sub = sub
	b.subAfter = afterID
	var once sync.Once
	cancel = func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if b.sub == sub {
				b.sub = nil
			}
		})
	}
	return replay, sub, cancel, nil
}

// Close ends the bus: the live subscriber's channel is closed and
// further Publish calls become no-ops.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	if b.sub != nil {
		close(b.sub)
		b.sub = nil
	}
}

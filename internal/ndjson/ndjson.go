// Package ndjson reads newline-delimited JSON frames — the MCP stdio
// transport — tolerating CRLF endings, blank lines and frames far larger
// than bufio's default limits.
package ndjson

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
)

// DefaultMaxFrame is the largest single frame the scanner will accept.
// Streamable HTTP has no framing limit, but an unbounded stdio line is a
// memory bomb; 32 MiB comfortably covers real tool results.
const DefaultMaxFrame = 32 << 20

// ErrFrameTooLarge is returned when one line exceeds the scanner's cap.
var ErrFrameTooLarge = errors.New("ndjson: frame exceeds maximum size")

// Scanner yields one frame (line) at a time. It is not safe for
// concurrent use.
type Scanner struct {
	r   *bufio.Reader
	max int
}

// NewScanner wraps r with the default frame cap.
func NewScanner(r io.Reader) *Scanner {
	return &Scanner{r: bufio.NewReaderSize(r, 64<<10), max: DefaultMaxFrame}
}

// SetMaxFrame overrides the frame cap; intended for tests.
func (s *Scanner) SetMaxFrame(n int) { s.max = n }

// Next returns the next non-blank frame with the trailing newline and any
// carriage return removed. It returns io.EOF when the stream ends, and a
// final unterminated frame is still delivered before EOF.
func (s *Scanner) Next() ([]byte, error) {
	for {
		var frame []byte
		for {
			chunk, err := s.r.ReadSlice('\n')
			frame = append(frame, chunk...)
			if len(frame) > s.max {
				return nil, fmt.Errorf("%w (%d bytes > %d)", ErrFrameTooLarge, len(frame), s.max)
			}
			if err == nil {
				break
			}
			if errors.Is(err, bufio.ErrBufferFull) {
				continue // long line: keep accumulating
			}
			if errors.Is(err, io.EOF) {
				if len(bytes.TrimSpace(frame)) == 0 {
					return nil, io.EOF
				}
				break // final line without a newline
			}
			return nil, err
		}
		frame = bytes.TrimRight(frame, "\r\n")
		if len(bytes.TrimSpace(frame)) == 0 {
			continue // skip blank lines between frames
		}
		return frame, nil
	}
}

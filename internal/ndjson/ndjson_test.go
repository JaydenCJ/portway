// Tests for the NDJSON frame scanner shared by both bridge directions.
package ndjson

import (
	"errors"
	"io"
	"strings"
	"testing"
)

func collect(t *testing.T, in string) []string {
	t.Helper()
	sc := NewScanner(strings.NewReader(in))
	var out []string
	for {
		frame, err := sc.Next()
		if errors.Is(err, io.EOF) {
			return out
		}
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		out = append(out, string(frame))
	}
}

func TestFramesSplitAndLineEndingsTrimmed(t *testing.T) {
	got := collect(t, "{\"a\":1}\n{\"b\":2}\r\n")
	if len(got) != 2 || got[0] != `{"a":1}` || got[1] != `{"b":2}` {
		t.Fatalf("got %q", got)
	}
}

func TestSkipsBlankLines(t *testing.T) {
	got := collect(t, "\n\n{\"a\":1}\n   \n\n{\"b\":2}\n\n")
	if len(got) != 2 {
		t.Fatalf("got %d frames: %q", len(got), got)
	}
}

func TestFinalFrameWithoutNewline(t *testing.T) {
	// A server that exits without flushing a trailing newline must not
	// lose its last message.
	got := collect(t, "{\"a\":1}\n{\"b\":2}")
	if len(got) != 2 || got[1] != `{"b":2}` {
		t.Fatalf("got %q", got)
	}
}

func TestEmptyInputIsEOF(t *testing.T) {
	sc := NewScanner(strings.NewReader(""))
	if _, err := sc.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestFrameSizeLimits(t *testing.T) {
	// 1 MiB is far beyond bufio's default 64 KiB read buffer: long
	// frames must survive the buffer boundary intact.
	big := `{"blob":"` + strings.Repeat("x", 1<<20) + `"}`
	got := collect(t, big+"\n")
	if len(got) != 1 || got[0] != big {
		t.Fatalf("long frame corrupted (len %d)", len(got[0]))
	}
	// But a frame beyond the cap is a memory bomb and must be refused.
	sc := NewScanner(strings.NewReader(strings.Repeat("y", 4096) + "\n"))
	sc.SetMaxFrame(1024)
	if _, err := sc.Next(); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("err = %v, want ErrFrameTooLarge", err)
	}
}

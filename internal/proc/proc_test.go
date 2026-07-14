// Tests for child-process plumbing, using only tiny POSIX shell
// commands (cat, sh) so they stay deterministic and offline.
package proc

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
)

// lockedBuffer lets the child's stderr be written from the reaper
// goroutine and read from the test without a race.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestSendAndReceiveThroughCat(t *testing.T) {
	c, err := Start([]string{"cat"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Stop()
	if err := c.Send([]byte(`{"jsonrpc":"2.0","id":1,"method":"ping"}`)); err != nil {
		t.Fatal(err)
	}
	got := <-c.Lines()
	if string(got) != `{"jsonrpc":"2.0","id":1,"method":"ping"}` {
		t.Fatalf("got %q", got)
	}
}

func TestLinesClosesOnChildExitAndBlanksSkipped(t *testing.T) {
	c, err := Start([]string{"sh", "-c", `printf '\n\n{"a":1}\n\n'`}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	first := <-c.Lines()
	if string(first) != `{"a":1}` {
		t.Fatalf("got %q", first)
	}
	if _, ok := <-c.Lines(); ok {
		t.Fatal("Lines not closed after child exit")
	}
	<-c.Done()
}

func TestExitCodeReported(t *testing.T) {
	c, err := Start([]string{"sh", "-c", "exit 3"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	<-c.Done()
	if code := c.ExitCode(); code != 3 {
		t.Fatalf("exit code = %d, want 3", code)
	}
}

func TestStopPreventsSendAndIsIdempotent(t *testing.T) {
	c, err := Start([]string{"cat"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	c.Stop()
	if err := c.Send([]byte("{}")); !errors.Is(err, ErrStopped) {
		t.Fatalf("err = %v, want ErrStopped", err)
	}
	c.Stop() // second Stop must not panic or hang
	// Stop after the child already exited on its own is also safe.
	c2, err := Start([]string{"sh", "-c", "exit 0"}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	<-c2.Done()
	c2.Stop()
}

func TestStderrPassesThrough(t *testing.T) {
	var stderr lockedBuffer
	c, err := Start([]string{"sh", "-c", `echo "diagnostic" >&2`}, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	<-c.Done() // Wait has returned, so stderr is fully flushed
	if got := stderr.String(); got != "diagnostic\n" {
		t.Fatalf("stderr = %q", got)
	}
}

func TestStartRejectsBadCommands(t *testing.T) {
	if _, err := Start(nil, io.Discard); err == nil {
		t.Fatal("empty command accepted")
	}
	if _, err := Start([]string{"/nonexistent-binary-for-portway-test"}, io.Discard); err == nil {
		t.Fatal("missing binary accepted")
	}
}

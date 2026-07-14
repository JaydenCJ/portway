// Package proc runs the wrapped stdio MCP server as a child process:
// serialized writes to its stdin, framed reads from its stdout, stderr
// passed through untouched, and a graceful stop (stdin close → wait →
// kill) that never leaves orphans behind.
package proc

import (
	"errors"
	"io"
	"os/exec"
	"sync"
	"time"

	"github.com/JaydenCJ/portway/internal/ndjson"
)

// StopGrace is how long Stop waits after closing stdin before killing
// the child. Well-behaved MCP servers exit on stdin EOF long before this.
const StopGrace = 3 * time.Second

// ErrStopped is returned by Send after the child has exited or Stop was
// called.
var ErrStopped = errors.New("proc: child is not running")

// Child is a running stdio server. Lines carries each stdout frame; it
// is closed when the child's stdout ends. Done is closed after the
// process has been fully reaped.
type Child struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser

	writeMu sync.Mutex
	stopped bool

	lines chan []byte
	done  chan struct{}

	exitMu   sync.Mutex
	exitCode int
}

// Start launches argv with stderr wired to the given writer. Frames the
// child writes to stdout appear on Lines(); anything on stderr passes
// through so server diagnostics stay visible.
func Start(argv []string, stderr io.Writer) (*Child, error) {
	if len(argv) == 0 {
		return nil, errors.New("proc: empty command")
	}
	cmd := exec.Command(argv[0], argv[1:]...)
	cmd.Stderr = stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	c := &Child{
		cmd:      cmd,
		stdin:    stdin,
		lines:    make(chan []byte, 16),
		done:     make(chan struct{}),
		exitCode: -1,
	}
	go c.readLoop(stdout)
	return c, nil
}

func (c *Child) readLoop(stdout io.Reader) {
	sc := ndjson.NewScanner(stdout)
	for {
		frame, err := sc.Next()
		if err != nil {
			break // io.EOF, oversized frame, or pipe error: stop reading
		}
		buf := append([]byte(nil), frame...)
		c.lines <- buf
	}
	close(c.lines)
	err := c.cmd.Wait()
	c.exitMu.Lock()
	if err == nil {
		c.exitCode = 0
	} else {
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			c.exitCode = exit.ExitCode()
		}
	}
	c.exitMu.Unlock()
	c.markStopped()
	close(c.done)
}

func (c *Child) markStopped() {
	c.writeMu.Lock()
	c.stopped = true
	c.writeMu.Unlock()
}

// Send writes one frame plus the newline terminator to the child's
// stdin. Writes are serialized so concurrent HTTP handlers can never
// interleave partial frames.
func (c *Child) Send(frame []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	if c.stopped {
		return ErrStopped
	}
	buf := make([]byte, 0, len(frame)+1)
	buf = append(buf, frame...)
	buf = append(buf, '\n')
	_, err := c.stdin.Write(buf)
	return err
}

// Lines yields each frame from the child's stdout; closed at stdout EOF.
func (c *Child) Lines() <-chan []byte { return c.lines }

// Done is closed once the process has exited and been reaped.
func (c *Child) Done() <-chan struct{} { return c.done }

// ExitCode reports the child's exit code, or -1 if it has not exited
// (or was killed by a signal with no code).
func (c *Child) ExitCode() int {
	c.exitMu.Lock()
	defer c.exitMu.Unlock()
	return c.exitCode
}

// Stop shuts the child down: close stdin (the stdio transport's shutdown
// signal), give it StopGrace to exit, then kill. Safe to call more than
// once and after the child has already exited.
func (c *Child) Stop() {
	c.writeMu.Lock()
	alreadyStopped := c.stopped
	c.stopped = true
	c.writeMu.Unlock()
	if !alreadyStopped {
		c.stdin.Close()
	}
	select {
	case <-c.done:
	case <-time.After(StopGrace):
		c.cmd.Process.Kill()
		<-c.done
	}
}

// Tests for the replayable event buffer behind the GET stream.
package bus

import (
	"errors"
	"testing"
)

func TestPublishAssignsIDsAndReplays(t *testing.T) {
	b := New(8)
	if id := b.Publish([]byte("a")); id != 1 {
		t.Fatalf("first id = %d", id)
	}
	if id := b.Publish([]byte("b")); id != 2 {
		t.Fatalf("second id = %d", id)
	}
	replay, _, cancel, err := b.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if len(replay) != 2 || string(replay[0].Data) != "a" || string(replay[1].Data) != "b" {
		t.Fatalf("replay = %v", replay)
	}
}

func TestSubscribeAfterIDSkipsDelivered(t *testing.T) {
	// This is exactly the Last-Event-ID resume path: the client saw
	// event 1 before its connection dropped.
	b := New(8)
	b.Publish([]byte("a"))
	b.Publish([]byte("b"))
	replay, _, cancel, err := b.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if len(replay) != 1 || string(replay[0].Data) != "b" {
		t.Fatalf("replay = %v", replay)
	}
}

func TestLiveDeliveryAfterSubscribe(t *testing.T) {
	b := New(8)
	_, ch, cancel, err := b.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	b.Publish([]byte("live"))
	env := <-ch
	if env.ID != 1 || string(env.Data) != "live" {
		t.Fatalf("env = %+v", env)
	}
}

func TestLiveDeliveryRespectsAfterID(t *testing.T) {
	// A resuming subscriber must never be handed an event it declared
	// already seen, even when the publish races its subscription.
	b := New(8)
	_, ch, cancel, err := b.Subscribe(1)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	b.Publish([]byte("already-seen")) // id 1
	b.Publish([]byte("fresh"))        // id 2
	env := <-ch
	if env.ID != 2 || string(env.Data) != "fresh" {
		t.Fatalf("got %+v, want only the fresh event", env)
	}
}

func TestSingleSubscriberEnforced(t *testing.T) {
	b := New(8)
	_, _, cancel, err := b.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := b.Subscribe(0); !errors.Is(err, ErrBusy) {
		t.Fatalf("err = %v, want ErrBusy", err)
	}
	cancel()
	cancel() // idempotent
	if _, _, cancel2, err := b.Subscribe(0); err != nil {
		t.Fatalf("resubscribe after cancel failed: %v", err)
	} else {
		cancel2()
	}
}

func TestBufferTrimsOldest(t *testing.T) {
	b := New(2)
	b.Publish([]byte("a"))
	b.Publish([]byte("b"))
	b.Publish([]byte("c"))
	replay, _, cancel, err := b.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if len(replay) != 2 || string(replay[0].Data) != "b" || replay[0].ID != 2 {
		t.Fatalf("replay = %v", replay)
	}
}

func TestCloseClosesSubscriberChannel(t *testing.T) {
	b := New(8)
	_, ch, cancel, err := b.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	b.Close()
	if _, ok := <-ch; ok {
		t.Fatal("channel not closed")
	}
}

func TestOperationsAfterClose(t *testing.T) {
	b := New(8)
	b.Publish([]byte("a"))
	b.Close()
	b.Close() // idempotent
	if id := b.Publish([]byte("x")); id != 0 {
		t.Fatalf("publish after close returned id %d", id)
	}
	replay, ch, cancel, err := b.Subscribe(0)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	if len(replay) != 0 {
		t.Fatalf("replay after close = %v", replay)
	}
	if _, ok := <-ch; ok {
		t.Fatal("channel from closed bus not closed")
	}
}

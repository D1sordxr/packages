package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

type fakeComponent struct {
	name string

	startErr    error
	shutdownErr error
	blockStart  bool

	events *eventLog
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (e *eventLog) add(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, s)
}

func (e *eventLog) snapshot() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.events...)
}

func (f *fakeComponent) Start(ctx context.Context) error {
	f.events.add("start:" + f.name)
	if f.startErr != nil {
		return f.startErr
	}
	if f.blockStart {
		<-ctx.Done()
	}
	return nil
}

func (f *fakeComponent) Shutdown(context.Context) error {
	f.events.add("shutdown:" + f.name)
	return f.shutdownErr
}

func TestAppShutsDownInReverseOrder(t *testing.T) {
	events := &eventLog{}
	a := &fakeComponent{name: "a", blockStart: true, events: events}
	b := &fakeComponent{name: "b", blockStart: true, events: events}
	c := &fakeComponent{name: "c", blockStart: true, events: events}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()

	if err := New(testLogger(), a, b, c).Run(ctx); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}

	got := events.snapshot()
	want := []string{"shutdown:c", "shutdown:b", "shutdown:a"}
	if len(got) < 3 {
		t.Fatalf("events = %v, want at least 3", got)
	}

	for i, w := range want {
		if got[len(got)-3+i] != w {
			t.Errorf("shutdown order = %v, want tail %v", got, want)
			break
		}
	}
}

func TestAppReturnsNilWhenComponentsStopCleanly(t *testing.T) {
	events := &eventLog{}
	comp := &fakeComponent{name: "quick", events: events}

	if err := New(testLogger(), comp).Run(context.Background()); err != nil {
		t.Fatalf("Run() = %v, want nil", err)
	}
}

func TestAppPropagatesStartError(t *testing.T) {
	events := &eventLog{}
	boom := errors.New("boom")
	comp := &fakeComponent{name: "bad", startErr: boom, events: events}

	err := New(testLogger(), comp).Run(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("Run() = %v, want error wrapping %v", err, boom)
	}
}

func TestAppPropagatesShutdownError(t *testing.T) {
	events := &eventLog{}
	boom := errors.New("cannot close")
	comp := &fakeComponent{name: "leaky", blockStart: true, shutdownErr: boom, events: events}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := New(testLogger(), comp).Run(ctx)
	if !errors.Is(err, boom) {
		t.Fatalf("Run() = %v, want error wrapping %v", err, boom)
	}
}

func TestAppShutsDownEveryComponentEvenWhenOneFails(t *testing.T) {
	events := &eventLog{}
	first := &fakeComponent{name: "first", blockStart: true, events: events}
	broken := &fakeComponent{name: "broken", blockStart: true, shutdownErr: errors.New("nope"), events: events}
	last := &fakeComponent{name: "last", blockStart: true, events: events}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_ = New(testLogger(), first, broken, last).Run(ctx)

	got := events.snapshot()
	for _, want := range []string{"shutdown:first", "shutdown:broken", "shutdown:last"} {
		found := false
		for _, e := range got {
			if e == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("missing %q in %v", want, got)
		}
	}
}

func TestWithShutdownTimeoutIgnoresNonPositive(t *testing.T) {
	app := New(testLogger()).With(WithShutdownTimeout(0))
	if app.shutdownTimeout != defaultShutdownTimeout {
		t.Errorf("shutdownTimeout = %v, want %v", app.shutdownTimeout, defaultShutdownTimeout)
	}

	app = New(testLogger()).With(WithShutdownTimeout(time.Second))
	if app.shutdownTimeout != time.Second {
		t.Errorf("shutdownTimeout = %v, want 1s", app.shutdownTimeout)
	}
}

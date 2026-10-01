package cron

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"
)

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
	return slices.Clone(e.events)
}

type fakeHandler struct {
	name    string
	stopErr error
	events  *eventLog
}

func (h *fakeHandler) Start(context.Context) error {
	h.events.add("start:" + h.name)
	return nil
}

func (h *fakeHandler) Stop(context.Context) error {
	h.events.add("stop:" + h.name)
	return h.stopErr
}

func TestWorkerStopsInReverseOrder(t *testing.T) {
	events := &eventLog{}
	boom := errors.New("boom")
	w := NewWorker(
		&fakeHandler{name: "a", events: events},
		&fakeHandler{name: "b", stopErr: boom, events: events},
	)

	done := make(chan error, 1)
	go func() { done <- w.Start(context.Background()) }()

	time.Sleep(10 * time.Millisecond)
	if err := w.Shutdown(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("Shutdown() = %v, want error wrapping %v", err, boom)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() = %v, want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Start() did not return after Shutdown")
	}

	want := []string{"start:a", "start:b", "stop:b", "stop:a"}
	if got := events.snapshot(); !slices.Equal(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
}

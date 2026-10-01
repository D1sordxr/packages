package httpserver

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"testing"
	"time"

	"github.com/D1sordxr/packages/app"
)

func TestServerServesUntilShutdown(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})
	srv := New(Config{Addr: "127.0.0.1:0"}, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/slow" {
			<-release
		}
		_, _ = io.WriteString(w, "ok")
	}))

	done := make(chan error, 1)
	go func() { done <- srv.Start(context.Background()) }()

	var base string
	for range 100 {
		if addr := srv.Addr(); addr != nil {
			base = "http://" + addr.String()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if base == "" {
		t.Fatal("server did not start listening")
	}

	resp, err := http.Get(base + "/")
	if err != nil {
		t.Fatalf("GET / = %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if string(body) != "ok" {
		t.Fatalf("body = %q, want ok", body)
	}

	// A request in flight during Shutdown is completed.
	slow := make(chan error, 1)
	go func() {
		resp, err := http.Get(base + "/slow")
		if err == nil {
			_ = resp.Body.Close()
		}
		slow <- err
	}()
	time.Sleep(50 * time.Millisecond)

	shutdownDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		shutdownDone <- srv.Shutdown(ctx)
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)

	if err = <-shutdownDone; err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if err = <-slow; err != nil {
		t.Fatalf("in-flight request failed: %v", err)
	}
	if err = <-done; err != nil {
		t.Fatalf("Start() = %v, want nil", err)
	}
}

func TestStartFailsOnBusyAddress(t *testing.T) {
	t.Parallel()

	first := New(Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler())
	go func() { _ = first.Start(context.Background()) }()
	t.Cleanup(func() { _ = first.Shutdown(context.Background()) })

	for first.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}

	second := New(Config{Addr: first.Addr().String()}, http.NotFoundHandler())
	if err := second.Start(context.Background()); err == nil {
		t.Fatal("Start() on a busy address = nil, want error")
	}
}

func TestStartReturnsWhenContextCancelled(t *testing.T) {
	t.Parallel()

	srv := New(Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Start(ctx) }()

	for srv.Addr() == nil {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start() = %v, want nil", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return after ctx was cancelled")
	}
}

type failingComponent struct{ err error }

func (f failingComponent) Start(context.Context) error {
	time.Sleep(50 * time.Millisecond)
	return f.err
}

func (failingComponent) Shutdown(context.Context) error { return nil }

func TestAppStopsWhenAnotherComponentFails(t *testing.T) {
	t.Parallel()

	boom := errors.New("boom")
	srv := New(Config{Addr: "127.0.0.1:0"}, http.NotFoundHandler())
	a := app.New(slog.New(slog.NewTextHandler(io.Discard, nil)), srv, failingComponent{err: boom})

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()

	select {
	case err := <-done:
		if !errors.Is(err, boom) {
			t.Fatalf("Run() = %v, want error wrapping %v", err, boom)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("app.Run() hangs after a component failed")
	}
}

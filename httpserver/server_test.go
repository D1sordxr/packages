package httpserver

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"
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

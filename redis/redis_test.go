package redis_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/D1sordxr/packages/redis"
)

// Integration tests: they run only when a Redis address is set.
const addrEnv = "PACKAGES_TEST_REDIS_ADDR"

func TestNewClientAndComponent(t *testing.T) {
	addr := os.Getenv(addrEnv)
	if addr == "" {
		t.Skipf("%s is not set", addrEnv)
	}

	client, err := redis.NewClient(context.Background(), &redis.Config{Addr: addr})
	if err != nil {
		t.Fatalf("NewClient() = %v", err)
	}

	comp := redis.NewClientComponent(client, slog.New(slog.NewTextHandler(io.Discard, nil)), 10*time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- comp.Start(context.Background()) }()

	time.Sleep(30 * time.Millisecond)
	if err = comp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if err = <-done; err != nil {
		t.Fatalf("Start() = %v", err)
	}
	if err = client.Ping(context.Background()).Err(); err == nil {
		t.Fatal("client still usable after Shutdown")
	}
}

func TestNewClientFailsWhenUnavailable(t *testing.T) {
	t.Parallel()

	_, err := redis.NewClient(context.Background(), &redis.Config{
		Addr:           "127.0.0.1:1",
		ConnectTimeout: 200 * time.Millisecond,
	})
	if err == nil {
		t.Fatal("NewClient() = nil, want error")
	}
}

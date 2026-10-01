package rabbitmq_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/D1sordxr/packages/rabbitmq"
)

// Integration tests: they run only when a broker URL is set.
const urlEnv = "PACKAGES_TEST_RABBITMQ_URL"

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func dial(t *testing.T) *amqp.Connection {
	t.Helper()

	url := os.Getenv(urlEnv)
	if url == "" {
		t.Skipf("%s is not set", urlEnv)
	}

	conn, err := rabbitmq.Dial(context.Background(), &rabbitmq.Config{URL: url, ConnectAttempts: 1})
	if err != nil {
		t.Fatalf("Dial() = %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return conn
}

// topology: main exchange -> main queue; delay exchange -> delay queue that
// dead-letters into the main exchange; dead exchange -> dead queue for
// rejected main-queue messages.
func declare(t *testing.T, conn *amqp.Connection) (main, delay, dead string) {
	t.Helper()

	prefix := "pkgtest." + t.Name() + "."
	main, delay, dead = prefix+"main", prefix+"delay", prefix+"dead"

	topology := rabbitmq.Topology{
		Exchanges: []rabbitmq.Exchange{
			{Name: main, Kind: amqp.ExchangeDirect, AutoDelete: true},
			{Name: delay, Kind: amqp.ExchangeDirect, AutoDelete: true},
			{Name: dead, Kind: amqp.ExchangeDirect, AutoDelete: true},
		},
		Queues: []rabbitmq.Queue{
			{Name: main, AutoDelete: true, Args: amqp.Table{rabbitmq.ArgDeadLetterExchange: dead}},
			{Name: delay, AutoDelete: true, Args: rabbitmq.DelayQueueArgs(main, "", 0)},
			{Name: dead, AutoDelete: true},
		},
		Bindings: []rabbitmq.Binding{
			{Queue: main, Exchange: main},
			{Queue: delay, Exchange: delay},
			{Queue: dead, Exchange: dead},
		},
	}

	if err := topology.Declare(conn); err != nil {
		t.Fatalf("Declare() = %v", err)
	}
	// Declaring twice must be a no-op.
	if err := topology.Declare(conn); err != nil {
		t.Fatalf("second Declare() = %v", err)
	}

	return main, delay, dead
}

func publisher(t *testing.T, conn *amqp.Connection) *rabbitmq.Publisher {
	t.Helper()

	p, err := rabbitmq.NewPublisher(conn)
	if err != nil {
		t.Fatalf("NewPublisher() = %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	return p
}

// consume runs a consumer and returns received bodies; it is shut down on cleanup.
func consume(t *testing.T, conn *amqp.Connection, queue string, handler rabbitmq.Handler) {
	t.Helper()

	c := rabbitmq.NewConsumer(conn, rabbitmq.ConsumerConfig{Queue: queue, Prefetch: 2}, handler, testLogger())

	done := make(chan error, 1)
	go func() { done <- c.Start(context.Background()) }()

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := c.Shutdown(ctx); err != nil {
			t.Errorf("Shutdown() = %v", err)
		}
		if err := <-done; err != nil {
			t.Errorf("Start() = %v", err)
		}
	})
}

func receive(t *testing.T, ch <-chan string, timeout time.Duration) string {
	t.Helper()

	select {
	case body := <-ch:
		return body
	case <-time.After(timeout):
		t.Fatal("no message received")
		return ""
	}
}

func TestPublishConsume(t *testing.T) {
	conn := dial(t)
	main, _, _ := declare(t, conn)

	got := make(chan string, 1)
	consume(t, conn, main, func(_ context.Context, d amqp.Delivery) error {
		got <- string(d.Body)
		return nil
	})

	if err := publisher(t, conn).Publish(context.Background(), main, "", amqp.Publishing{Body: []byte("hello")}); err != nil {
		t.Fatalf("Publish() = %v", err)
	}

	if body := receive(t, got, 5*time.Second); body != "hello" {
		t.Fatalf("body = %q, want hello", body)
	}
}

func TestDelayQueueDeadLettersIntoMain(t *testing.T) {
	conn := dial(t)
	main, delay, _ := declare(t, conn)

	got := make(chan string, 1)
	consume(t, conn, main, func(_ context.Context, d amqp.Delivery) error {
		got <- string(d.Body)
		return nil
	})

	const ttl = 300 * time.Millisecond
	start := time.Now()
	msg := amqp.Publishing{Body: []byte("later"), Expiration: rabbitmq.Expiration(ttl)}
	if err := publisher(t, conn).Publish(context.Background(), delay, "", msg); err != nil {
		t.Fatalf("Publish() = %v", err)
	}

	if body := receive(t, got, 5*time.Second); body != "later" {
		t.Fatalf("body = %q, want later", body)
	}
	if elapsed := time.Since(start); elapsed < ttl {
		t.Fatalf("delivered after %v, want at least %v", elapsed, ttl)
	}
}

func TestHandlerErrorRejectsToDeadLetter(t *testing.T) {
	conn := dial(t)
	main, _, dead := declare(t, conn)

	var calls atomic.Int32
	consume(t, conn, main, func(context.Context, amqp.Delivery) error {
		if calls.Add(1) == 1 {
			return errors.New("boom")
		}
		panic("boom again")
	})

	got := make(chan string, 2)
	consume(t, conn, dead, func(_ context.Context, d amqp.Delivery) error {
		got <- string(d.Body)
		return nil
	})

	p := publisher(t, conn)
	for _, body := range []string{"error", "panic"} {
		if err := p.Publish(context.Background(), main, "", amqp.Publishing{Body: []byte(body)}); err != nil {
			t.Fatalf("Publish() = %v", err)
		}
	}

	received := map[string]bool{receive(t, got, 5*time.Second): true, receive(t, got, 5*time.Second): true}
	if !received["error"] || !received["panic"] {
		t.Fatalf("dead-lettered = %v, want error and panic", received)
	}
}

func TestShutdownWaitsForInFlightDelivery(t *testing.T) {
	conn := dial(t)
	main, _, _ := declare(t, conn)

	started := make(chan struct{})
	var finished atomic.Bool
	c := rabbitmq.NewConsumer(conn, rabbitmq.ConsumerConfig{Queue: main}, func(context.Context, amqp.Delivery) error {
		close(started)
		time.Sleep(200 * time.Millisecond)
		finished.Store(true)
		return nil
	}, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Start(ctx) }()

	if err := publisher(t, conn).Publish(context.Background(), main, "", amqp.Publishing{Body: []byte("slow")}); err != nil {
		t.Fatalf("Publish() = %v", err)
	}
	<-started
	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := c.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if !finished.Load() {
		t.Fatal("Shutdown returned before the in-flight delivery was handled")
	}
	if err := <-done; err != nil {
		t.Fatalf("Start() = %v", err)
	}
}

func TestConnectionComponentFailsWhenConnectionDrops(t *testing.T) {
	conn := dial(t)
	comp := rabbitmq.NewConnectionComponent(conn)

	done := make(chan error, 1)
	go func() { done <- comp.Start(context.Background()) }()

	// Closing the connection behind the component's back stands in for a broker failure.
	time.Sleep(50 * time.Millisecond)
	_ = conn.Close()

	select {
	case err := <-done:
		if !errors.Is(err, rabbitmq.ErrConnectionClosed) {
			t.Fatalf("Start() = %v, want %v", err, rabbitmq.ErrConnectionClosed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start() did not return after the connection closed")
	}
}

func TestConnectionComponentShutdown(t *testing.T) {
	conn := dial(t)
	comp := rabbitmq.NewConnectionComponent(conn)

	done := make(chan error, 1)
	go func() { done <- comp.Start(context.Background()) }()
	time.Sleep(50 * time.Millisecond)

	if err := comp.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("Start() = %v, want nil after Shutdown", err)
	}
	if !conn.IsClosed() {
		t.Fatal("connection is still open after Shutdown")
	}
}

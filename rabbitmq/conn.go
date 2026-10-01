package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Dial connects to the broker, retrying while it is unavailable
// (ConnectAttempts times with ConnectBackoff between attempts).
func Dial(ctx context.Context, cfg *Config) (*amqp.Connection, error) {
	const op = "rabbitmq.Dial"

	var lastErr error
	for attempt := 1; attempt <= cfg.attempts(); attempt++ {
		conn, err := amqp.Dial(cfg.ConnectionString())
		if err == nil {
			return conn, nil
		}
		lastErr = err

		if attempt == cfg.attempts() {
			break
		}

		timer := time.NewTimer(cfg.backoff())
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, fmt.Errorf("%s: %w", op, ctx.Err())
		case <-timer.C:
		}
	}

	return nil, fmt.Errorf("%s: after %d attempts: %w", op, cfg.attempts(), lastErr)
}

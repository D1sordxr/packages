package redis

import (
	"context"
	"sync"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

const (
	defaultHealthInterval = 20 * time.Second
	healthTimeout         = time.Second
)

// Logger is what ClientComponent needs to report failed health checks.
// *slog.Logger satisfies it as is.
type Logger interface {
	Error(msg string, args ...any)
}

// ClientComponent ties the client to the application lifecycle (app.Component):
// while running it pings Redis periodically, on shutdown it closes the client.
// Register it before the components that use Redis: app stops components in
// reverse order, so the client is closed last.
type ClientComponent struct {
	client   goredis.UniversalClient
	log      Logger
	interval time.Duration

	stop     chan struct{}
	stopOnce sync.Once
}

// NewClientComponent creates a ClientComponent. A non-positive interval means
// the default one (20s).
func NewClientComponent(client goredis.UniversalClient, log Logger, interval time.Duration) *ClientComponent {
	if interval <= 0 {
		interval = defaultHealthInterval
	}

	return &ClientComponent{
		client:   client,
		log:      log,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

func (c *ClientComponent) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.ping(ctx); err != nil {
				c.log.Error("redis health check failed", "error", err.Error())
			}
		case <-ctx.Done():
			return nil
		case <-c.stop:
			return nil
		}
	}
}

func (c *ClientComponent) Shutdown(context.Context) error {
	var err error
	c.stopOnce.Do(func() {
		close(c.stop)
		err = c.client.Close()
	})

	return err
}

func (c *ClientComponent) ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	return c.client.Ping(pingCtx).Err()
}

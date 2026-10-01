package postgres

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHealthInterval = 20 * time.Second
	healthTimeout         = time.Second
)

// Logger is what PoolComponent needs to report failed health checks.
// *slog.Logger satisfies it as is.
type Logger interface {
	Error(msg string, args ...any)
}

// PoolComponent ties the pool to the application lifecycle (app.Component):
// while running it pings the database periodically, on shutdown it closes the pool.
// Register it before the components that use the database: app stops
// components in reverse order, so the pool is closed last.
type PoolComponent struct {
	pool     *pgxpool.Pool
	log      Logger
	interval time.Duration

	stop     chan struct{}
	stopOnce sync.Once
}

// NewPoolComponent creates a PoolComponent. A non-positive interval means
// the default one (20s).
func NewPoolComponent(pool *pgxpool.Pool, log Logger, interval time.Duration) *PoolComponent {
	if interval <= 0 {
		interval = defaultHealthInterval
	}

	return &PoolComponent{
		pool:     pool,
		log:      log,
		interval: interval,
		stop:     make(chan struct{}),
	}
}

func (c *PoolComponent) Start(ctx context.Context) error {
	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := c.ping(ctx); err != nil {
				c.log.Error("postgres health check failed", "error", err.Error())
			}
		case <-ctx.Done():
			return nil
		case <-c.stop:
			return nil
		}
	}
}

func (c *PoolComponent) Shutdown(context.Context) error {
	c.stopOnce.Do(func() {
		close(c.stop)
		c.pool.Close()
	})

	return nil
}

func (c *PoolComponent) ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, healthTimeout)
	defer cancel()

	return c.pool.Ping(pingCtx)
}

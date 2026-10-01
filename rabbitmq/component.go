package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrConnectionClosed = errors.New("rabbitmq connection closed")

// Logger is what the package needs to report background failures.
// *slog.Logger satisfies it as is.
type Logger interface {
	Error(msg string, args ...any)
}

// ConnectionComponent ties the connection to the application lifecycle
// (app.Component): Start fails when the broker drops the connection, so the
// application stops instead of running without a broker; Shutdown closes it.
// Register it before the components that use the connection: app stops
// components in reverse order, so the connection is closed last.
type ConnectionComponent struct {
	conn *amqp.Connection

	stop     chan struct{}
	stopOnce sync.Once
}

func NewConnectionComponent(conn *amqp.Connection) *ConnectionComponent {
	return &ConnectionComponent{
		conn: conn,
		stop: make(chan struct{}),
	}
}

func (c *ConnectionComponent) Start(ctx context.Context) error {
	closed := c.conn.NotifyClose(make(chan *amqp.Error, 1))

	select {
	case amqpErr, ok := <-closed:
		select {
		case <-c.stop:
			return nil
		default:
		}
		if ok && amqpErr != nil {
			return fmt.Errorf("%w: %s", ErrConnectionClosed, amqpErr.Error())
		}
		return ErrConnectionClosed
	case <-ctx.Done():
		return nil
	case <-c.stop:
		return nil
	}
}

func (c *ConnectionComponent) Shutdown(context.Context) error {
	var err error
	c.stopOnce.Do(func() {
		close(c.stop)
		if !c.conn.IsClosed() {
			err = c.conn.Close()
		}
	})

	return err
}

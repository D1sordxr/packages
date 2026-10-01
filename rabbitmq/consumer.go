package rabbitmq

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrDeliveriesClosed = errors.New("rabbitmq deliveries channel closed")

const defaultPrefetch = 1

// Handler processes a delivery. A nil error acks it; an error or a panic
// rejects it without requeue, so it goes to the queue's dead-letter
// exchange, if one is configured, or is dropped.
type Handler func(ctx context.Context, d amqp.Delivery) error

type ConsumerConfig struct {
	Queue string
	// Tag identifies the consumer in the broker; empty means a generated one.
	Tag string
	// Prefetch is the number of unacked deliveries the broker sends ahead and
	// the number of deliveries handled concurrently. Zero means 1.
	Prefetch int
}

// Consumer runs a Handler over a queue (app.Component). Start blocks until
// ctx is cancelled or Shutdown is called; deliveries already received are
// handled to completion before Start returns.
type Consumer struct {
	conn    *amqp.Connection
	cfg     ConsumerConfig
	handler Handler
	log     Logger

	mu       sync.Mutex
	ch       *amqp.Channel // set once subscribed
	finished chan struct{} // closed when Start returns after subscribing

	stop     chan struct{}
	stopOnce sync.Once
}

func NewConsumer(conn *amqp.Connection, cfg ConsumerConfig, handler Handler, log Logger) *Consumer {
	if cfg.Prefetch <= 0 {
		cfg.Prefetch = defaultPrefetch
	}
	if cfg.Tag == "" {
		cfg.Tag = cfg.Queue + "-" + rand.Text()
	}

	return &Consumer{
		conn:     conn,
		cfg:      cfg,
		handler:  handler,
		log:      log,
		finished: make(chan struct{}),
		stop:     make(chan struct{}),
	}
}

func (c *Consumer) Start(ctx context.Context) error {
	const op = "rabbitmq.Consumer.Start"

	deliveries, err := c.subscribe()
	if err != nil {
		return fmt.Errorf("%s: queue %q: %w", op, c.cfg.Queue, err)
	}
	defer close(c.finished)
	defer c.closeChannel()

	// Handlers get a context detached from Start's ctx: a delivery that is
	// being handled when the application stops is finished, not interrupted.
	handleCtx := context.WithoutCancel(ctx)

	// The deliveries channel is closed by the library when the consumer is
	// cancelled or the channel/connection is closed.
	var workers sync.WaitGroup
	for range c.cfg.Prefetch {
		workers.Go(func() {
			for d := range deliveries {
				c.handle(handleCtx, d)
			}
		})
	}

	drained := make(chan struct{})
	go func() {
		workers.Wait()
		close(drained)
	}()

	select {
	case <-drained:
		select {
		case <-c.stop:
			return nil
		default:
			return fmt.Errorf("%s: queue %q: %w", op, c.cfg.Queue, ErrDeliveriesClosed)
		}
	case <-ctx.Done():
	case <-c.stop:
	}

	c.cancel()
	<-drained

	return nil
}

func (c *Consumer) subscribe() (<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if err = ch.Qos(c.cfg.Prefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("set qos: %w", err)
	}

	deliveries, err := ch.Consume(c.cfg.Queue, c.cfg.Tag, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("consume: %w", err)
	}

	c.ch = ch
	return deliveries, nil
}

func (c *Consumer) handle(ctx context.Context, d amqp.Delivery) {
	if err := c.safeHandle(ctx, d); err != nil {
		c.log.Error("rabbitmq handler failed, rejecting delivery",
			"queue", c.cfg.Queue, "message_id", d.MessageId, "error", err.Error())

		if nackErr := d.Nack(false, false); nackErr != nil {
			c.log.Error("rabbitmq nack failed", "queue", c.cfg.Queue, "error", nackErr.Error())
		}
		return
	}

	if err := d.Ack(false); err != nil {
		c.log.Error("rabbitmq ack failed", "queue", c.cfg.Queue, "error", err.Error())
	}
}

func (c *Consumer) safeHandle(ctx context.Context, d amqp.Delivery) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v\n%s", r, debug.Stack())
		}
	}()

	return c.handler(ctx, d)
}

// cancel stops new deliveries; the ones already received are still handled.
func (c *Consumer) cancel() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ch != nil && !c.ch.IsClosed() {
		_ = c.ch.Cancel(c.cfg.Tag, false)
	}
}

func (c *Consumer) closeChannel() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ch != nil && !c.ch.IsClosed() {
		_ = c.ch.Close()
	}
}

// Shutdown stops consuming and waits, within ctx, for the deliveries already
// received to be handled.
func (c *Consumer) Shutdown(ctx context.Context) error {
	c.stopOnce.Do(func() { close(c.stop) })

	c.mu.Lock()
	subscribed := c.ch != nil
	c.mu.Unlock()
	if !subscribed {
		// Start has not subscribed yet; if it does, it sees stop and returns.
		return nil
	}

	select {
	case <-c.finished:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("rabbitmq.Consumer.Shutdown: %w", ctx.Err())
	}
}

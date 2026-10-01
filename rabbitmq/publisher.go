package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var ErrNotConfirmed = errors.New("message was not confirmed by the broker")

// Publisher publishes messages on its own channel in confirm mode: Publish
// returns once the broker has taken responsibility for the message.
// It is safe for concurrent use and reopens the channel if it was closed
// (e.g. by a channel-level error), as long as the connection is alive.
type Publisher struct {
	conn *amqp.Connection

	mu sync.Mutex
	ch *amqp.Channel
}

func NewPublisher(conn *amqp.Connection) (*Publisher, error) {
	const op = "rabbitmq.NewPublisher"

	p := &Publisher{conn: conn}
	if _, err := p.channel(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return p, nil
}

func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, msg amqp.Publishing) error {
	const op = "rabbitmq.Publisher.Publish"

	confirm, err := p.publish(ctx, exchange, routingKey, msg)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	acked, err := confirm.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("%s: wait confirm: %w", op, err)
	}
	if !acked {
		return fmt.Errorf("%s: %w", op, ErrNotConfirmed)
	}

	return nil
}

func (p *Publisher) publish(
	ctx context.Context,
	exchange, routingKey string,
	msg amqp.Publishing,
) (*amqp.DeferredConfirmation, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	ch, err := p.channel()
	if err != nil {
		return nil, err
	}

	return ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, msg)
}

// channel returns an open confirm-mode channel; p.mu must be held,
// except in the constructor.
func (p *Publisher) channel() (*amqp.Channel, error) {
	if p.ch != nil && !p.ch.IsClosed() {
		return p.ch, nil
	}

	ch, err := p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if err = ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}

	p.ch = ch
	return ch, nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.ch == nil || p.ch.IsClosed() {
		return nil
	}

	return p.ch.Close()
}

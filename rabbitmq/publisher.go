package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrNotConfirmed  = errors.New("message was not confirmed by the broker")
	ErrUnroutable    = errors.New("message was returned by the broker as unroutable")
	ErrChannelClosed = errors.New("publisher channel closed before the broker confirmed the message")
)

// HeaderPublishSeq is set by Publisher on every message to match broker
// returns to publishes. Consumers can ignore it.
const HeaderPublishSeq = "x-publish-seq"

// Publisher publishes messages as mandatory on its own channel in confirm
// mode: Publish returns nil once the broker has routed the message to at
// least one queue and taken responsibility for it, ErrUnroutable when no
// queue matched, and ErrNotConfirmed when the broker nacked it.
// It is safe for concurrent use and reopens the channel if it was closed
// (e.g. by a channel-level error), as long as the connection is alive.
type Publisher struct {
	conn *amqp.Connection

	mu sync.Mutex
	cc *confirmChannel
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

	cc, seq, done, err := p.publish(ctx, exchange, routingKey, msg)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	select {
	case err = <-done:
		if err != nil {
			return fmt.Errorf("%s: exchange %q, routing key %q: %w", op, exchange, routingKey, err)
		}
		return nil
	case <-ctx.Done():
		cc.forget(seq)
		return fmt.Errorf("%s: wait confirm: %w", op, ctx.Err())
	}
}

func (p *Publisher) publish(
	ctx context.Context,
	exchange, routingKey string,
	msg amqp.Publishing,
) (*confirmChannel, uint64, <-chan error, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	cc, err := p.channel()
	if err != nil {
		return nil, 0, nil, err
	}

	// p.mu serializes publishes on the channel, so the next sequence number
	// is the delivery tag this message gets.
	seq := cc.ch.GetNextPublishSeqNo()

	done, err := cc.register(seq)
	if err != nil {
		return nil, 0, nil, err
	}

	headers := make(amqp.Table, len(msg.Headers)+1)
	maps.Copy(headers, msg.Headers)
	headers[HeaderPublishSeq] = int64(seq) //nolint:gosec // sequence numbers do not overflow int64
	msg.Headers = headers

	if err = cc.ch.PublishWithContext(ctx, exchange, routingKey, true, false, msg); err != nil {
		cc.forget(seq)
		return nil, 0, nil, err
	}

	return cc, seq, done, nil
}

// channel returns an open confirm-mode channel; p.mu must be held,
// except in the constructor.
func (p *Publisher) channel() (*confirmChannel, error) {
	if p.cc != nil && !p.cc.ch.IsClosed() {
		return p.cc, nil
	}

	cc, err := newConfirmChannel(p.conn)
	if err != nil {
		return nil, err
	}

	p.cc = cc
	return cc, nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cc == nil || p.cc.ch.IsClosed() {
		return nil
	}

	return p.cc.ch.Close()
}

// confirmChannel tracks publishes awaiting a broker confirmation on one channel.
type confirmChannel struct {
	ch *amqp.Channel

	mu      sync.Mutex
	pending map[uint64]*pendingPublish
	closed  bool
}

type pendingPublish struct {
	done     chan error // buffered: the listener never blocks on it
	returned *amqp.Return
}

func newConfirmChannel(conn *amqp.Connection) (*confirmChannel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}
	if err = ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}

	// Both channels are unbuffered on purpose. The broker sends basic.return
	// before basic.ack of the same message, and the library hands them over in
	// that order, blocking until each is received. With a single unbuffered
	// listener, the return is recorded before the confirmation is resolved.
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation))
	returns := ch.NotifyReturn(make(chan amqp.Return))

	cc := &confirmChannel{
		ch:      ch,
		pending: make(map[uint64]*pendingPublish),
	}
	go cc.listen(confirms, returns)

	return cc, nil
}

func (cc *confirmChannel) register(seq uint64) (<-chan error, error) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	if cc.closed {
		return nil, ErrChannelClosed
	}

	p := &pendingPublish{done: make(chan error, 1)}
	cc.pending[seq] = p

	return p.done, nil
}

func (cc *confirmChannel) forget(seq uint64) {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	delete(cc.pending, seq)
}

// listen must keep draining both channels: the library blocks its reader
// until each confirmation and return is received.
func (cc *confirmChannel) listen(confirms <-chan amqp.Confirmation, returns <-chan amqp.Return) {
	for confirms != nil {
		select {
		case r, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			cc.markReturned(r)
		case c, ok := <-confirms:
			if !ok {
				confirms = nil
				continue
			}
			cc.resolve(c)
		}
	}

	cc.failAll()
}

func (cc *confirmChannel) markReturned(r amqp.Return) {
	seq, ok := r.Headers[HeaderPublishSeq].(int64)
	if !ok {
		return
	}

	cc.mu.Lock()
	defer cc.mu.Unlock()

	if p, found := cc.pending[uint64(seq)]; found { //nolint:gosec // set from a uint64 by publish
		p.returned = &r
	}
}

func (cc *confirmChannel) resolve(c amqp.Confirmation) {
	cc.mu.Lock()
	p, found := cc.pending[c.DeliveryTag]
	delete(cc.pending, c.DeliveryTag)
	cc.mu.Unlock()

	if !found {
		return
	}

	switch {
	case !c.Ack:
		p.done <- ErrNotConfirmed
	case p.returned != nil:
		p.done <- fmt.Errorf("%w: %d %s", ErrUnroutable, p.returned.ReplyCode, p.returned.ReplyText)
	default:
		p.done <- nil
	}
}

func (cc *confirmChannel) failAll() {
	cc.mu.Lock()
	defer cc.mu.Unlock()

	cc.closed = true
	for seq, p := range cc.pending {
		p.done <- ErrChannelClosed
		delete(cc.pending, seq)
	}
}

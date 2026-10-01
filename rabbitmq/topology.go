package rabbitmq

import (
	"fmt"
	"strconv"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Well-known queue arguments.
const (
	ArgDeadLetterExchange   = "x-dead-letter-exchange"
	ArgDeadLetterRoutingKey = "x-dead-letter-routing-key"
	ArgMessageTTL           = "x-message-ttl"
)

type Exchange struct {
	Name       string
	Kind       string // amqp.ExchangeDirect, amqp.ExchangeTopic, ...
	Durable    bool
	AutoDelete bool
	Args       amqp.Table
}

type Queue struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	Args       amqp.Table
}

type Binding struct {
	Queue      string
	Exchange   string
	RoutingKey string
	Args       amqp.Table
}

// Topology is a set of exchanges, queues and bindings declared together.
// Declaration is idempotent as long as the definitions do not change.
type Topology struct {
	Exchanges []Exchange
	Queues    []Queue
	Bindings  []Binding
}

// Declare declares the topology on a dedicated channel: a failed declaration
// closes the channel, so it must not be shared with publishers or consumers.
func (t Topology) Declare(conn *amqp.Connection) error {
	const op = "rabbitmq.Topology.Declare"

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("%s: open channel: %w", op, err)
	}
	defer func() { _ = ch.Close() }()

	for _, e := range t.Exchanges {
		if err = ch.ExchangeDeclare(e.Name, e.Kind, e.Durable, e.AutoDelete, false, false, e.Args); err != nil {
			return fmt.Errorf("%s: exchange %q: %w", op, e.Name, err)
		}
	}

	for _, q := range t.Queues {
		if _, err = ch.QueueDeclare(q.Name, q.Durable, q.AutoDelete, q.Exclusive, false, q.Args); err != nil {
			return fmt.Errorf("%s: queue %q: %w", op, q.Name, err)
		}
	}

	for _, b := range t.Bindings {
		if err = ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, false, b.Args); err != nil {
			return fmt.Errorf("%s: bind %q to %q: %w", op, b.Queue, b.Exchange, err)
		}
	}

	return nil
}

// DelayQueueArgs returns the arguments of a delay queue: messages expire
// after ttl (when ttl > 0; otherwise only per-message TTL applies) and are
// dead-lettered to exchange with routingKey (or their original key when empty).
//
// RabbitMQ expires messages only at the head of a queue, so a message with
// a long per-message TTL delays the ones behind it. Keep TTLs in one queue
// close to each other, or use a queue-level ttl.
func DelayQueueArgs(exchange, routingKey string, ttl time.Duration) amqp.Table {
	args := amqp.Table{ArgDeadLetterExchange: exchange}
	if routingKey != "" {
		args[ArgDeadLetterRoutingKey] = routingKey
	}
	if ttl > 0 {
		args[ArgMessageTTL] = ttl.Milliseconds()
	}

	return args
}

// Expiration formats a per-message TTL for amqp.Publishing.Expiration.
// Non-positive durations expire immediately.
func Expiration(ttl time.Duration) string {
	return strconv.FormatInt(max(ttl.Milliseconds(), 0), 10)
}

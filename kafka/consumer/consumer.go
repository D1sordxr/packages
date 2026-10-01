package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers        []string      `yaml:"brokers" binding:"required"`
	GroupID        string        `yaml:"group_id"`
	CommitInterval time.Duration `yaml:"commit_interval" env-default:"1000ms"`
}

type Handler interface {
	Handle(ctx context.Context, msg kafka.Message) error
}

type Logger interface {
	Info(msg string)
	Error(msg string)
}

type Consumer struct {
	Reader  *kafka.Reader
	Handler Handler
	log     Logger
}

func NewConsumer(config *Config, topic string, handler Handler, log Logger) *Consumer {
	if len(config.Brokers) == 0 || topic == "" {
		panic("invalid config data")
	}

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        config.Brokers,
		GroupID:        config.GroupID,
		Topic:          topic,
		CommitInterval: config.CommitInterval,
	})

	return &Consumer{
		Reader:  reader,
		Handler: handler,
		log:     log,
	}
}

// Consume method reads and handles message from kafka
// Prefer using in goroutine
func (c *Consumer) Consume(ctx context.Context, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}
	defer c.log.Info("Consumer shutting down...")

	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, err := c.Reader.ReadMessage(ctx)
			if err != nil {
				c.log.Error(fmt.Sprintf("error reading message: %v", err))
				continue
			}

			if err = c.Handler.Handle(ctx, m); err != nil {
				c.log.Error(fmt.Sprintf("error processing message: %v", err))
			}
		}
	}
}

func (c *Consumer) Close() {
	if err := c.Reader.Close(); err != nil {
		c.log.Error(fmt.Sprintf("error closing kafka reader: %v", err))
	}
}

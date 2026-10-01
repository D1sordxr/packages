package producer

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Brokers      []string      `yaml:"brokers" binding:"required"`
	BatchSize    int           `yaml:"batch_size" env-default:"10"`
	BatchTimeout time.Duration `yaml:"batch_timeout" env-default:"500ms"`
}

type IProducer interface {
	SendMessage(ctx context.Context, topic string, key []byte, value []byte) error
}

type Producer struct {
	Writer *kafka.Writer
}

func NewProducer(config *Config) *Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
	}

	return &Producer{Writer: writer}
}

func (p *Producer) SendMessage(
	ctx context.Context,
	topic string,
	key []byte,
	value []byte,
) error {
	return p.Writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

func (p *Producer) Close() error {
	return p.Writer.Close()
}

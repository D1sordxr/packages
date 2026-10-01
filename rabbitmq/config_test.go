package rabbitmq

import (
	"testing"
	"time"
)

func TestConfigConnectionString(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "url wins",
			cfg:  Config{URL: "amqp://u:p@h:1/v", Host: "ignored"},
			want: "amqp://u:p@h:1/v",
		},
		{
			name: "default vhost",
			cfg:  Config{Host: "rabbit", Port: 5672, Username: "admin", Password: "secret", VHost: "/"},
			want: "amqp://admin:secret@rabbit:5672/",
		},
		{
			name: "named vhost and escaped password",
			cfg:  Config{Host: "rabbit", Port: 5672, Username: "admin", Password: "p@ss/word", VHost: "orders"},
			want: "amqp://admin:p%40ss%2Fword@rabbit:5672/orders",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.cfg.ConnectionString(); got != tt.want {
				t.Fatalf("ConnectionString() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestExpiration(t *testing.T) {
	t.Parallel()

	for in, want := range map[time.Duration]string{
		1500 * time.Millisecond: "1500",
		0:                       "0",
		-time.Second:            "0",
	} {
		if got := Expiration(in); got != want {
			t.Errorf("Expiration(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestDelayQueueArgs(t *testing.T) {
	t.Parallel()

	args := DelayQueueArgs("main", "", 0)
	if args[ArgDeadLetterExchange] != "main" {
		t.Errorf("dead-letter exchange = %v, want main", args[ArgDeadLetterExchange])
	}
	if _, ok := args[ArgDeadLetterRoutingKey]; ok {
		t.Error("routing key set for empty value")
	}
	if _, ok := args[ArgMessageTTL]; ok {
		t.Error("ttl set for zero value")
	}

	args = DelayQueueArgs("main", "key", 30*time.Second)
	if args[ArgDeadLetterRoutingKey] != "key" || args[ArgMessageTTL] != int64(30000) {
		t.Errorf("args = %v", args)
	}
}

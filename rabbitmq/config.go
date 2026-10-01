// Package rabbitmq provides a RabbitMQ connection, topology declaration,
// a confirming publisher and a consumer that fits the app lifecycle.
package rabbitmq

import (
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	defaultConnectAttempts = 5
	defaultConnectBackoff  = 2 * time.Second
)

// Config holds connection settings. URL is used as is when set;
// otherwise the URL is built from Host/Port/Username/Password/VHost.
type Config struct {
	URL      string `yaml:"url"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	VHost    string `yaml:"vhost"`

	// ConnectAttempts and ConnectBackoff control Dial retries while the broker
	// is starting. Zero values mean 5 attempts with a 2s pause.
	ConnectAttempts int           `yaml:"connect_attempts"`
	ConnectBackoff  time.Duration `yaml:"connect_backoff"`
}

func (c *Config) ConnectionString() string {
	if c.URL != "" {
		return c.URL
	}

	u := url.URL{
		Scheme: "amqp",
		User:   url.UserPassword(c.Username, c.Password),
		Host:   net.JoinHostPort(c.Host, strconv.Itoa(c.Port)),
		// An empty path selects the default vhost "/".
		Path: "/" + strings.TrimPrefix(c.VHost, "/"),
	}

	return u.String()
}

func (c *Config) attempts() int {
	if c.ConnectAttempts > 0 {
		return c.ConnectAttempts
	}
	return defaultConnectAttempts
}

func (c *Config) backoff() time.Duration {
	if c.ConnectBackoff > 0 {
		return c.ConnectBackoff
	}
	return defaultConnectBackoff
}

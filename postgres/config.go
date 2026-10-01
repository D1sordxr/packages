// Package postgres provides a pgx connection pool and its application lifecycle.
package postgres

import (
	"net"
	"net/url"
	"strconv"
	"time"
)

const defaultConnectTimeout = 5 * time.Second

// Config holds connection and pool settings. DSN is used as is when set;
// otherwise a URL is built from Host/Port/Database/User/Password and SSLMode
// (when empty, the libpq default "prefer" applies). Zero Port means 5432.
// Zero pool settings keep the pgx defaults.
type Config struct {
	DSN      string `yaml:"dsn"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	Database string `yaml:"database"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	SSLMode  string `yaml:"ssl_mode"`

	MaxConns        int32         `yaml:"max_conns"`
	MinConns        int32         `yaml:"min_conns"`
	MaxConnLifetime time.Duration `yaml:"max_conn_lifetime"`
	MaxConnIdleTime time.Duration `yaml:"max_conn_idle_time"`
	ConnectTimeout  time.Duration `yaml:"connect_timeout"`
}

func (c *Config) ConnectionString() string {
	if c.DSN != "" {
		return c.DSN
	}

	host := c.Host
	if c.Port != 0 {
		host = net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
	}

	// url.URL escapes credentials and the database name, so values with
	// spaces, quotes or '@' survive.
	u := url.URL{
		Scheme: "postgres",
		Host:   host,
		Path:   "/" + c.Database,
	}
	if c.User != "" {
		u.User = url.UserPassword(c.User, c.Password)
	}
	if c.SSLMode != "" {
		u.RawQuery = url.Values{"sslmode": {c.SSLMode}}.Encode()
	}

	return u.String()
}

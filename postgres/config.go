// Package postgres provides a pgx connection pool and its application lifecycle.
package postgres

import (
	"fmt"
	"time"
)

const defaultConnectTimeout = 5 * time.Second

// Config holds connection and pool settings. DSN is used as is when set;
// otherwise the connection string is built from Host/Port/Database/User/Password
// and SSLMode (when empty, the libpq default "prefer" applies).
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

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d",
		c.Host, c.User, c.Password, c.Database, c.Port,
	)
	if c.SSLMode != "" {
		dsn += " sslmode=" + c.SSLMode
	}

	return dsn
}

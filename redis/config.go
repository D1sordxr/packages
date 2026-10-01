// Package redis provides a go-redis client built from Config and its application lifecycle.
package redis

import "time"

const defaultConnectTimeout = 5 * time.Second

// Config holds connection settings. Zero values keep the go-redis defaults.
type Config struct {
	Addr     string `yaml:"addr"`
	Username string `yaml:"username"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`

	PoolSize       int           `yaml:"pool_size"`
	DialTimeout    time.Duration `yaml:"dial_timeout"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
	ConnectTimeout time.Duration `yaml:"connect_timeout"`
}

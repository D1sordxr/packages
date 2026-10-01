// Package log builds slog loggers that pick up request-scoped fields from the context.
package log

import (
	"fmt"
	"io"
	"log/slog"
)

type Format string

const (
	FormatJSON Format = "json"
	FormatText Format = "text"
)

// Config selects the level and the output format. An empty Level means info,
// an empty Format means JSON.
type Config struct {
	Level     string `yaml:"level"`
	Format    Format `yaml:"format"`
	AddSource bool   `yaml:"add_source"`
}

// New returns a logger writing to w. Its handler adds the fields stored in the
// context by Inject and Add to every record logged with a *Context method.
func New(cfg Config, w io.Writer) (*slog.Logger, error) {
	const op = "log.New"

	level, err := ParseLevel(cfg.Level)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	opts := &slog.HandlerOptions{Level: level, AddSource: cfg.AddSource}

	var handler slog.Handler
	switch cfg.Format {
	case FormatJSON, "":
		handler = slog.NewJSONHandler(w, opts)
	case FormatText:
		handler = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("%s: unknown format %q", op, cfg.Format)
	}

	return slog.New(NewContextHandler(handler)), nil
}

// ParseLevel accepts the names slog understands: debug, info, warn, error,
// in any case and with an optional offset such as "warn+2". An empty string
// means info.
func ParseLevel(s string) (slog.Level, error) {
	if s == "" {
		return slog.LevelInfo, nil
	}

	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("parse level %q: %w", s, err)
	}

	return level, nil
}

// Discard returns a logger that drops every record.
func Discard() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

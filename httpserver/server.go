// Package httpserver runs a net/http server as an app.Component.
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	defaultReadHeaderTimeout = 5 * time.Second
	defaultShutdownTimeout   = 10 * time.Second
)

// Config holds the listen address and timeouts. Zero timeouts mean none,
// except ReadHeaderTimeout, which defaults to 5s, and ShutdownTimeout, which
// defaults to 10s.
type Config struct {
	Addr              string        `yaml:"addr"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
	ReadTimeout       time.Duration `yaml:"read_timeout"`
	WriteTimeout      time.Duration `yaml:"write_timeout"`
	IdleTimeout       time.Duration `yaml:"idle_timeout"`
	// ShutdownTimeout bounds the graceful shutdown Start performs when its ctx
	// is cancelled; connections still active after it are closed.
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// Server is an http.Server with the app.Component lifecycle: Start listens
// and serves until its ctx is cancelled or Shutdown is called. Both stop
// accepting connections and wait for active requests: Shutdown within its
// ctx deadline, a cancelled Start within Config.ShutdownTimeout.
type Server struct {
	srv             *http.Server
	shutdownTimeout time.Duration

	mu       sync.Mutex
	listener net.Listener
}

func New(cfg Config, handler http.Handler) *Server {
	readHeaderTimeout := cfg.ReadHeaderTimeout
	if readHeaderTimeout <= 0 {
		readHeaderTimeout = defaultReadHeaderTimeout
	}

	shutdownTimeout := cfg.ShutdownTimeout
	if shutdownTimeout <= 0 {
		shutdownTimeout = defaultShutdownTimeout
	}

	return &Server{
		shutdownTimeout: shutdownTimeout,
		srv: &http.Server{
			Addr:              cfg.Addr,
			Handler:           handler,
			ReadHeaderTimeout: readHeaderTimeout,
			ReadTimeout:       cfg.ReadTimeout,
			WriteTimeout:      cfg.WriteTimeout,
			IdleTimeout:       cfg.IdleTimeout,
		},
	}
}

func (s *Server) Start(ctx context.Context) error {
	const op = "httpserver.Server.Start"

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("%s: %w", op, err)
	}

	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()

	// Serve does not watch ctx: without this, a cancelled ctx (e.g. another
	// component failed) would leave Start running and the app hanging.
	stopWatching := context.AfterFunc(ctx, s.shutdownOnCancel)
	defer stopWatching()

	if err = s.srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("%s: %w", op, err)
	}

	return nil
}

// Addr returns the address the server listens on, or nil before Start has
// opened the listener. Useful with Config.Addr ":0".
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) shutdownOnCancel() {
	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()

	if err := s.srv.Shutdown(ctx); err != nil {
		_ = s.srv.Close()
	}
}

func (s *Server) Shutdown(ctx context.Context) error {
	if err := s.srv.Shutdown(ctx); err != nil {
		return fmt.Errorf("httpserver.Server.Shutdown: %w", err)
	}

	return nil
}

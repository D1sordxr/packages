// Package app manages the application lifecycle as a set of Components.
package app

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
)

const defaultShutdownTimeout = 15 * time.Second

var ErrShutdownTimeout = errors.New("timed out waiting for components to stop")

type Option func(*App)

func WithShutdownTimeout(d time.Duration) Option {
	return func(a *App) {
		if d > 0 {
			a.shutdownTimeout = d
		}
	}
}

// App starts components concurrently and stops them in reverse order when
// ctx is cancelled, when any component fails, or when all of them have exited.
type App struct {
	log             Logger
	components      []Component
	shutdownTimeout time.Duration
}

func New(log Logger, components ...Component) *App {
	return &App{
		log:             log,
		components:      components,
		shutdownTimeout: defaultShutdownTimeout,
	}
}

func (a *App) With(opts ...Option) *App {
	for _, opt := range opts {
		opt(a)
	}
	return a
}

func (a *App) Run(ctx context.Context) error {
	a.log.Info("App starting", "components", len(a.components))

	errGroup, groupCtx := errgroup.WithContext(ctx)
	for _, component := range a.components {
		c := component
		errGroup.Go(func() error {
			if err := c.Start(groupCtx); err != nil {
				return fmt.Errorf("component %T: %w", c, err)
			}
			return nil
		})
	}

	stopped := make(chan error, 1)
	go func() { stopped <- errGroup.Wait() }()

	var (
		runErr  error
		drained bool
	)

	select {
	case err := <-stopped:
		drained = true
		if err != nil {
			runErr = err
			a.log.Error("App component failed", "error", err.Error())
		} else {
			a.log.Info("All app components stopped on their own")
		}
	case <-ctx.Done():
		a.log.Info("App received a terminate signal")
	}

	return errors.Join(runErr, a.shutdown(stopped, drained))
}

func (a *App) shutdown(stopped <-chan error, drained bool) error {
	a.log.Info("App shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.shutdownTimeout)
	defer cancel()

	errs := make([]error, 0, len(a.components)+1)
	for _, c := range slices.Backward(a.components) {
		if err := c.Shutdown(shutdownCtx); err != nil {
			errs = append(errs, fmt.Errorf("shutdown %T: %w", c, err))
		}
	}

	if !drained {
		select {
		case err := <-stopped:
			if err != nil {
				errs = append(errs, err)
			}
		case <-shutdownCtx.Done():
			errs = append(errs, ErrShutdownTimeout)
		}
	}

	if len(errs) == 0 {
		a.log.Info("App shutdown complete")
		return nil
	}

	joined := errors.Join(errs...)
	a.log.Error("App shutdown with errors", "errors", joined.Error())
	return joined
}

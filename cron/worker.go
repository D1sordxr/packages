package cron

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Worker starts handlers in order and stops them in reverse order.
// It implements app.Component.
type Worker struct {
	handlers []Handler

	stop     chan struct{}
	stopOnce sync.Once
}

func NewWorker(hs ...Handler) *Worker {
	return &Worker{
		handlers: hs,
		stop:     make(chan struct{}),
	}
}

func (w *Worker) Start(ctx context.Context) error {
	for idx, h := range w.handlers {
		if err := h.Start(ctx); err != nil {
			return fmt.Errorf("start handler %d (%T): %w", idx, h, err)
		}
	}

	select {
	case <-ctx.Done():
	case <-w.stop:
	}

	return nil
}

func (w *Worker) Shutdown(ctx context.Context) error {
	w.stopOnce.Do(func() { close(w.stop) })

	var errs []error
	for idx := len(w.handlers) - 1; idx >= 0; idx-- {
		h := w.handlers[idx]
		if err := h.Stop(ctx); err != nil {
			errs = append(errs, fmt.Errorf("stop handler %d (%T): %w", idx, h, err))
		}
	}

	return errors.Join(errs...)
}

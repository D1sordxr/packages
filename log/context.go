package log

import (
	"context"
	"log/slog"
	"slices"
	"sync"

	"github.com/D1sordxr/packages/ctxutil"
)

// fields is stored in the context by its type via ctxutil; being unexported,
// no other package can store or replace it.
type fields struct {
	mu   sync.Mutex
	args []any
}

// Inject returns a context carrying a new mutable set of fields: the fields
// already in ctx followed by args. Code further down the call chain extends
// the set with Add, and the caller of Inject sees those additions too.
func Inject(ctx context.Context, args ...any) context.Context {
	inherited := Fields(ctx)

	return ctxutil.WithValue(ctx, &fields{args: append(inherited, args...)})
}

// Add appends args to the set created by the nearest Inject. Without one it
// does nothing.
func Add(ctx context.Context, args ...any) {
	f, ok := ctxutil.Lookup[*fields](ctx)
	if !ok {
		return
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	f.args = append(f.args, args...)
}

// Fields returns a copy of the fields stored in ctx, as alternating keys and
// values or slog.Attr, ready to pass to a slog method.
func Fields(ctx context.Context) []any {
	f, ok := ctxutil.Lookup[*fields](ctx)
	if !ok {
		return nil
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.args)
}

// NewContextHandler wraps h so that every record handled with a context gets
// the fields stored in that context.
func NewContextHandler(h slog.Handler) slog.Handler {
	return contextHandler{Handler: h}
}

type contextHandler struct {
	slog.Handler
}

func (h contextHandler) Handle(ctx context.Context, r slog.Record) error {
	if args := Fields(ctx); len(args) > 0 {
		r = r.Clone()
		r.Add(args...)
	}

	return h.Handler.Handle(ctx, r)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{Handler: h.Handler.WithGroup(name)}
}

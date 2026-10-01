// Package ctxutil stores values in context in a type-safe way, keyed by their type.
package ctxutil

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrNotFoundInContext  = errors.New("not found in context")
	ErrWrongTypeInContext = errors.New("context value has wrong type")
)

type key[T any] struct{}

func WithValue[T any](ctx context.Context, value T) context.Context {
	return context.WithValue(ctx, key[T]{}, value)
}

// Lookup returns the value of type T stored in ctx. Unlike Value, it does not
// allocate when the value is missing, so it suits hot paths such as logging.
func Lookup[T any](ctx context.Context) (T, bool) {
	v, ok := ctx.Value(key[T]{}).(T)
	return v, ok
}

func Value[T any](ctx context.Context) (T, error) {
	const op = "pkg.ctxutil.Value"

	if v, ok := Lookup[T](ctx); ok {
		return v, nil
	}

	var zero T
	if val := ctx.Value(key[T]{}); val != nil {
		return zero, fmt.Errorf("%s: got %T, expected %T: %w", op, val, zero, ErrWrongTypeInContext)
	}

	return zero, fmt.Errorf("%s: value of type %T: %w", op, zero, ErrNotFoundInContext)
}

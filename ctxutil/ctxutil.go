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

func Value[T any](ctx context.Context) (T, error) {
	const op = "pkg.ctxutil.Value"

	val := ctx.Value(key[T]{})
	if val == nil {
		var zero T
		return zero, fmt.Errorf("%s: value of type %T: %w", op, zero, ErrNotFoundInContext)
	}

	v, ok := val.(T)
	if !ok {
		var zero T
		return zero, fmt.Errorf("%s: got %T, expected %T: %w", op, val, zero, ErrWrongTypeInContext)
	}

	return v, nil
}

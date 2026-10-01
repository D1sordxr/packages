package ctxutil

import (
	"context"
	"errors"
	"testing"
)

func TestWithValue(t *testing.T) {
	t.Parallel()

	ctx := WithValue(context.Background(), "hello")

	if got := ctx.Value(key[string]{}); got != "hello" {
		t.Fatalf("ctx.Value = %v, want %q", got, "hello")
	}
}

func TestValue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		in      context.Context
		want    string
		wantErr error
	}{
		{
			name: "basic case",
			in:   WithValue(context.Background(), "hello"),
			want: "hello",
		},
		{
			name:    "not found",
			in:      context.Background(),
			wantErr: ErrNotFoundInContext,
		},
		{
			name: "override value",
			in:   WithValue(WithValue(context.Background(), "first"), "second"),
			want: "second",
		},
		{
			name:    "wrong type",
			in:      context.WithValue(context.Background(), key[string]{}, 42),
			wantErr: ErrWrongTypeInContext,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := Value[string](tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Value() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("Value() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLookup(t *testing.T) {
	t.Parallel()

	if got, ok := Lookup[string](WithValue(context.Background(), "hello")); !ok || got != "hello" {
		t.Fatalf("Lookup() = %q, %v, want %q, true", got, ok, "hello")
	}

	if got, ok := Lookup[string](context.Background()); ok || got != "" {
		t.Fatalf("Lookup() on empty ctx = %q, %v, want \"\", false", got, ok)
	}

	wrong := context.WithValue(context.Background(), key[string]{}, 42)
	if _, ok := Lookup[string](wrong); ok {
		t.Fatal("Lookup() of a wrong type = true, want false")
	}
}

func TestLookupMissDoesNotAllocate(t *testing.T) {
	ctx := WithValue(context.Background(), 1)

	if allocs := testing.AllocsPerRun(100, func() { _, _ = Lookup[string](ctx) }); allocs != 0 {
		t.Fatalf("Lookup() miss allocates %v times, want 0", allocs)
	}
}

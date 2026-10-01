package app

import "context"

// Component is a unit of the application lifecycle.
//
// Start blocks while the component is running and returns once ctx is
// cancelled or Shutdown is called. Shutdown releases resources and must
// respect the deadline of the given ctx.
type Component interface {
	Start(ctx context.Context) error
	Shutdown(ctx context.Context) error
}

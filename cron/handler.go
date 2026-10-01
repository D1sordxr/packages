// Package cron runs a group of background jobs as a single app.Component.
package cron

import "context"

// Handler is a background job, e.g. a wrapper around robfig/cron.
type Handler interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

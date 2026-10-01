package tx

import (
	"context"
	"errors"

	exec "github.com/D1sordxr/packages/postgres/executor"
)

// WithTransaction runs fn in a transaction stored in ctx.
//
// If ctx already carries a transaction, fn joins it: commit and rollback are
// up to whoever started it. An error returned by fn or a panic rolls the
// transaction back.
func (m *ManagerImpl) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	_, err := m.GetTxExecutor(ctx)
	switch {
	case err == nil:
		return fn(ctx)
	case !errors.Is(err, exec.ErrTxNotFoundInContext):
		return err
	}

	tx, err := m.GetPoolExecutor().Begin(ctx)
	if err != nil {
		return err
	}

	// After a successful Commit, Rollback returns pgx.ErrTxClosed, which is ignored.
	// WithoutCancel: the rollback must reach the database even if ctx is cancelled.
	defer func() { _ = tx.Rollback(context.WithoutCancel(ctx)) }()

	ctxWithTx, err := m.InjectTxExecutor(ctx, tx)
	if err != nil {
		return err
	}

	if err = fn(ctxWithTx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// Package executor provides database access aware of a transaction stored in context.
package executor

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/D1sordxr/packages/ctxutil"
)

//go:generate ifacemaker -f *.go -s ExecutorImpl -i Executor -p executor -o executor_interface.go

//go:generate gowrap gen -g -p . -i Executor -t ../../gowrap/errwrap.tmpl -o executor_with_errwrap.go -v "OpPrefix=postgres.Executor"

// ExecutorImpl resolves a query executor from ctx: the transaction if one
// is stored in ctx, the pool otherwise.
// noinspection GoNameStartsWithPackageName
type ExecutorImpl struct {
	pool *pgxpool.Pool
}

func NewExecutor(pool *pgxpool.Pool) *ExecutorImpl {
	return &ExecutorImpl{
		pool: pool,
	}
}

func (e *ExecutorImpl) GetExecutor(ctx context.Context) QueryExecutor {
	if tx, err := e.GetTxExecutor(ctx); err == nil {
		return tx
	}
	return e.pool
}

func (e *ExecutorImpl) GetPoolExecutor() *pgxpool.Pool {
	return e.pool
}

func (e *ExecutorImpl) GetTxExecutor(ctx context.Context) (pgx.Tx, error) {
	tx, err := ctxutil.Value[pgx.Tx](ctx)
	if err != nil {
		if errors.Is(err, ctxutil.ErrNotFoundInContext) {
			return nil, ErrTxNotFoundInContext
		}
		return nil, err
	}
	return tx, nil
}

func (e *ExecutorImpl) InjectTxExecutor(ctx context.Context, tx pgx.Tx) (context.Context, error) {
	if _, err := e.GetTxExecutor(ctx); err == nil {
		return ctx, fmt.Errorf("%T already exists in context", tx)
	}
	return ctxutil.WithValue[pgx.Tx](ctx, tx), nil
}

func (e *ExecutorImpl) ExecBatch(ctx context.Context, batch *pgx.Batch) error {
	br := e.GetExecutor(ctx).SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for range batch.Len() {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}

	return nil
}

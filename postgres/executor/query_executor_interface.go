package executor

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type QueryExecutor interface {
	Exec(ctx context.Context, sql string, arguments ...interface{}) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	CopyFrom(
		ctx context.Context,
		tableName pgx.Identifier,
		columnNames []string,
		rowSrc pgx.CopyFromSource,
	) (int64, error)
}

// Querier is the narrow database access for application code: it returns the
// query executor bound to ctx (the transaction, if one is active). It is
// intentionally narrower than Executor: the pool, manual transactions and
// batches are the infrastructure's concern, not the application's.
type Querier interface {
	GetExecutor(ctx context.Context) QueryExecutor
}

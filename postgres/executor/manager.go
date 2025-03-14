package executor

import (
	"context"
	"errors"
	"github.com/D1sordxr/packages/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Executor defines the interface for executing database operations.
type Executor interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, optionsAndArgs ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, optionsAndArgs ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// txKey and batchKey are used as keys for storing transactions and batches in the context.
type (
	txKey    struct{}
	batchKey struct{}
)

// Manager of the Executor interface.
// It is used to manage transactions and batches in the context and delegate queries to the appropriate executor.
type Manager struct {
	*postgres.Pool
}

// NewManager creates a new Manager instance with the given Postgres connection pool.
func NewManager(pool *postgres.Pool) *Manager {
	return &Manager{Pool: pool}
}

// InjectTx stores a transaction in the context for later retrieval.
func (m *Manager) InjectTx(ctx context.Context, tx pgx.Tx) context.Context {
	return context.WithValue(ctx, txKey{}, tx)
}

// ExtractTx retrieves a transaction from the context.
// It returns the transaction and a boolean indicating whether a transaction was found.
func (m *Manager) ExtractTx(ctx context.Context) (pgx.Tx, bool) {
	tx, ok := ctx.Value(txKey{}).(pgx.Tx)
	return tx, ok
}

// NewBatch creates a new BatchExecutor for queueing batch queries.
func (m *Manager) NewBatch() *BatchExecutor {
	return &BatchExecutor{Batch: &pgx.Batch{}}
}

// InjectBatch stores a batch in the context for later retrieval.
func (m *Manager) InjectBatch(ctx context.Context, batch *BatchExecutor) context.Context {
	return context.WithValue(ctx, batchKey{}, batch)
}

// ExtractBatch retrieves a batch from the context, if it exists.
func (m *Manager) ExtractBatch(ctx context.Context) (*BatchExecutor, bool) {
	batch, ok := ctx.Value(batchKey{}).(*BatchExecutor)
	return batch, ok
}

// GetExecutor returns the appropriate executor based on the context.
// If a batch is present in the context, it returns the batch executor.
// If a transaction is present, it returns the transaction.
// Otherwise, it returns a PoolExecutor, which wraps the connection pool.
func (m *Manager) GetExecutor(ctx context.Context) Executor {
	if batch, ok := m.ExtractBatch(ctx); ok {
		return batch
	}

	if tx, ok := m.ExtractTx(ctx); ok {
		return tx
	}

	return &PoolExecutor{Pool: m.Pool}
}

// GetPoolExecutor returns a PoolExecutor that wraps the connection pool.
// It can be used to execute queries outside a transaction or batch.
// Prefer using GetExecutor instead of this method.
func (m *Manager) GetPoolExecutor() Executor {
	return &PoolExecutor{Pool: m.Pool}
}

// GetTxExecutor returns the transaction executor from the context.
// Prefer using GetExecutor instead of this method.
func (m *Manager) GetTxExecutor(ctx context.Context) (Executor, error) {
	tx, ok := m.ExtractTx(ctx)
	if !ok {
		return tx, nil
	}

	return tx, errors.New("no transaction found in context")
}

// GetBatchExecutor returns the batch executor from the context.
// Prefer using GetExecutor instead of this method.
func (m *Manager) GetBatchExecutor(ctx context.Context) (Executor, error) {
	batch, ok := m.ExtractBatch(ctx)
	if !ok {
		return batch, nil
	}

	return batch, errors.New("no batch found in context")
}

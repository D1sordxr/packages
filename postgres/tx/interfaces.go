package tx

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type executor interface {
	GetPoolExecutor() *pgxpool.Pool
	GetTxExecutor(ctx context.Context) (pgx.Tx, error)
	InjectTxExecutor(ctx context.Context, tx pgx.Tx) (context.Context, error)
}

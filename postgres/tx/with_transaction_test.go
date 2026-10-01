package tx_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/D1sordxr/packages/postgres"
	exec "github.com/D1sordxr/packages/postgres/executor"
	"github.com/D1sordxr/packages/postgres/tx"
)

// Integration tests: they run only when a test database DSN is set.
const dsnEnv = "PACKAGES_TEST_POSTGRES_DSN"

var errBoom = errors.New("boom")

type fixture struct {
	pool *pgxpool.Pool
	exec exec.Executor
	tx   tx.Manager
}

func setup(t *testing.T) fixture {
	t.Helper()

	dsn := os.Getenv(dsnEnv)
	if dsn == "" {
		t.Skipf("%s is not set", dsnEnv)
	}

	ctx := context.Background()

	pool, err := postgres.NewPool(ctx, &postgres.Config{DSN: dsn})
	if err != nil {
		t.Fatalf("NewPool() = %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err = pool.Exec(ctx, `DROP TABLE IF EXISTS tx_test; CREATE TABLE tx_test (v int)`); err != nil {
		t.Fatalf("create table: %v", err)
	}

	e := exec.NewExecutorWithErrWrap(exec.NewExecutor(pool))

	return fixture{
		pool: pool,
		exec: e,
		tx:   tx.NewManagerWithErrWrap(tx.NewManager(e)),
	}
}

func (f fixture) insert(ctx context.Context, v int) error {
	_, err := f.exec.GetExecutor(ctx).Exec(ctx, `INSERT INTO tx_test (v) VALUES ($1)`, v)
	return err
}

func (f fixture) count(t *testing.T) int {
	t.Helper()

	var n int
	if err := f.pool.QueryRow(context.Background(), `SELECT count(*) FROM tx_test`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}

	return n
}

func TestWithTransactionCommits(t *testing.T) {
	f := setup(t)

	err := f.tx.WithTransaction(context.Background(), func(ctx context.Context) error {
		if err := f.tx.IsTx(ctx); err != nil {
			t.Errorf("IsTx() inside transaction = %v, want nil", err)
		}
		return f.insert(ctx, 1)
	})
	if err != nil {
		t.Fatalf("WithTransaction() = %v", err)
	}

	if got := f.count(t); got != 1 {
		t.Fatalf("rows = %d, want 1", got)
	}
}

func TestWithTransactionRollsBackOnError(t *testing.T) {
	f := setup(t)

	err := f.tx.WithTransaction(context.Background(), func(ctx context.Context) error {
		if err := f.insert(ctx, 1); err != nil {
			return err
		}
		return errBoom
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("WithTransaction() = %v, want %v", err, errBoom)
	}

	if got := f.count(t); got != 0 {
		t.Fatalf("rows = %d, want 0", got)
	}
}

func TestWithTransactionRollsBackOnPanic(t *testing.T) {
	f := setup(t)

	func() {
		defer func() { _ = recover() }()

		_ = f.tx.WithTransaction(context.Background(), func(ctx context.Context) error {
			_ = f.insert(ctx, 1)
			panic("boom")
		})
	}()

	if got := f.count(t); got != 0 {
		t.Fatalf("rows = %d, want 0", got)
	}
}

func TestNestedWithTransactionJoinsOuter(t *testing.T) {
	f := setup(t)

	err := f.tx.WithTransaction(context.Background(), func(ctx context.Context) error {
		if err := f.insert(ctx, 1); err != nil {
			return err
		}

		if err := f.tx.WithTransaction(ctx, func(ctx context.Context) error {
			return f.insert(ctx, 2)
		}); err != nil {
			return err
		}

		// The nested call must not commit: nothing is visible outside yet.
		if got := f.count(t); got != 0 {
			t.Errorf("rows visible before outer commit = %d, want 0", got)
		}

		return nil
	})
	if err != nil {
		t.Fatalf("WithTransaction() = %v", err)
	}

	if got := f.count(t); got != 2 {
		t.Fatalf("rows = %d, want 2", got)
	}
}

func TestNestedErrorRollsBackOuter(t *testing.T) {
	f := setup(t)

	err := f.tx.WithTransaction(context.Background(), func(ctx context.Context) error {
		if err := f.insert(ctx, 1); err != nil {
			return err
		}

		return f.tx.WithTransaction(ctx, func(context.Context) error { return errBoom })
	})
	if !errors.Is(err, errBoom) {
		t.Fatalf("WithTransaction() = %v, want %v", err, errBoom)
	}

	if got := f.count(t); got != 0 {
		t.Fatalf("rows = %d, want 0", got)
	}
}

func TestIsTxOutsideTransaction(t *testing.T) {
	f := setup(t)

	if err := f.tx.IsTx(context.Background()); !errors.Is(err, exec.ErrTxNotFoundInContext) {
		t.Fatalf("IsTx() = %v, want %v", err, exec.ErrTxNotFoundInContext)
	}
}

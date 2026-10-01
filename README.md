# packages

Reusable infrastructure for Go services: application lifecycle, PostgreSQL pool with
context-bound transactions, cron workers, Kafka and logging helpers.

```sh
go get github.com/D1sordxr/packages@latest
```

Requires Go 1.27.

## Packages

| Package | Purpose |
|---|---|
| `app` | Runs `Component`s concurrently, stops them in reverse order with a timeout |
| `postgres` | `pgxpool` construction from `Config`, `PoolComponent` for health checks and graceful close |
| `postgres/executor` | Resolves the query executor from context: the active transaction or the pool |
| `postgres/tx` | Transaction manager: `WithTransaction(ctx, fn)` |
| `cron` | `Worker` that runs a group of background handlers as one `Component` |
| `ctxutil` | Type-safe context values keyed by type |
| `kafka/consumer`, `kafka/producer` | Thin wrappers over `segmentio/kafka-go` |
| `log` | zap-based logger |
| `gowrap` | `gowrap` template that wraps returned errors with the operation name |

## Usage

```go
import (
	"github.com/D1sordxr/packages/app"
	"github.com/D1sordxr/packages/postgres"
	exec "github.com/D1sordxr/packages/postgres/executor"
	"github.com/D1sordxr/packages/postgres/tx"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := slog.Default()

	pool, err := postgres.NewPool(ctx, &postgres.Config{DSN: os.Getenv("DSN")})
	if err != nil {
		log.Error("init postgres", "error", err)
		os.Exit(1)
	}

	executor := exec.NewExecutor(pool)
	txManager := tx.NewManager(executor)

	// Repositories depend on exec.Querier, use cases on tx.Manager.
	_ = txManager

	// Components are stopped in reverse order: register the pool first
	// so it is closed after everything that uses it.
	a := app.New(log,
		postgres.NewPoolComponent(pool, log, 0),
		httpServer, // any app.Component
	).With(app.WithShutdownTimeout(10 * time.Second))

	if err := a.Run(ctx); err != nil {
		log.Error("app stopped", "error", err)
		os.Exit(1)
	}
}
```

### Components

```go
type Component interface {
	Start(ctx context.Context) error    // blocks until ctx is cancelled or Shutdown is called
	Shutdown(ctx context.Context) error // releases resources within ctx deadline
}
```

`App` stops when `ctx` is cancelled, when any component returns an error, or when all
components exit on their own. `app.Logger` is satisfied by `*slog.Logger`.

### Transactions

Repositories take the executor bound to the current context, so the same code works
inside and outside a transaction:

```go
type Repo struct{ q exec.Querier }

func (r *Repo) Create(ctx context.Context, v int) error {
	_, err := r.q.GetExecutor(ctx).Exec(ctx, `INSERT INTO t (v) VALUES ($1)`, v)
	return err
}
```

```go
err := txManager.WithTransaction(ctx, func(ctx context.Context) error {
	if err := repo.Create(ctx, 1); err != nil {
		return err // rolls back
	}
	return other.Do(ctx) // nested WithTransaction calls join this transaction
})
```

- An error or panic in `fn` rolls the transaction back.
- A nested `WithTransaction` joins the outer transaction; only the outermost call commits.

### Error-wrapping decorators

`executor` and `tx` ship `*WithErrWrap` decorators generated from `gowrap/errwrap.tmpl`.
They prefix errors with the operation name and keep `errors.Is` working:

```go
executor := exec.NewExecutorWithErrWrap(exec.NewExecutor(pool))
txManager := tx.NewManagerWithErrWrap(tx.NewManager(executor))
```

To use the template in your own module:

```go
//go:generate gowrap gen -g -p . -i UseCase -t $GOWRAP_TPL -o usecase_with_errwrap.go -v "OpPrefix=orders.UseCase"
```

```sh
GOWRAP_TPL=$(go list -m -f '{{.Dir}}' github.com/D1sordxr/packages)/gowrap/errwrap.tmpl go generate ./...
```

## Development

```sh
go test -race ./...
```

`postgres/tx` integration tests run only when a database is available:

```sh
docker run -d --rm --name pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16-alpine
PACKAGES_TEST_POSTGRES_DSN='postgres://postgres:test@localhost:55432/postgres?sslmode=disable' \
	go test -race ./postgres/...
```

Code generation requires [`ifacemaker`](https://github.com/vburenin/ifacemaker) and
[`gowrap`](https://github.com/hexdigest/gowrap) in `PATH`:

```sh
go generate ./...
```

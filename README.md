# packages

Reusable infrastructure for Go services: application lifecycle, PostgreSQL pool with
context-bound transactions, RabbitMQ, Redis, HTTP server, cron workers, Kafka and logging helpers.

```sh
go get github.com/D1sordxr/packages@v0.4.0
```

Requires Go 1.27. Upgrading? See [Upgrading from v0.3](#upgrading-from-v03) and
[Upgrading from v0.2](#upgrading-from-v02).

## Packages

| Package | Purpose |
|---|---|
| `app` | Runs `Component`s concurrently, stops them in reverse order with a timeout |
| `postgres` | `pgxpool` construction from `Config`, `PoolComponent` for health checks and graceful close |
| `postgres/executor` | Resolves the query executor from context: the active transaction or the pool |
| `postgres/tx` | Transaction manager: `WithTransaction(ctx, fn)` |
| `rabbitmq` | Connection with retries, topology declaration, confirming `Publisher`, `Consumer` and `ConnectionComponent` |
| `redis` | `go-redis` client construction from `Config`, `ClientComponent` for health checks and graceful close |
| `httpserver` | `net/http` server as a `Component`; graceful shutdown on `Shutdown` or when its context is cancelled |
| `cron` | `Worker` that runs a group of background handlers as one `Component` |
| `ctxutil` | Type-safe context values keyed by type |
| `kafka/consumer`, `kafka/producer` | Thin wrappers over `segmentio/kafka-go` |
| `log` | `*slog.Logger` from `Config`, request-scoped fields carried in the context, errors with log fields |
| `gowrap` | `gowrap` template that wraps returned errors with the operation name |

## Usage

```go
import (
	"github.com/D1sordxr/packages/app"
	"github.com/D1sordxr/packages/httpserver"
	"github.com/D1sordxr/packages/postgres"
	exec "github.com/D1sordxr/packages/postgres/executor"
	"github.com/D1sordxr/packages/postgres/tx"
	"github.com/D1sordxr/packages/redis"
)

func main() {
	if err := run(); err != nil {
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := slog.Default()

	pool, err := postgres.NewPool(ctx, &postgres.Config{
		Host: "localhost", Database: "app", User: "app", Password: "secret", SSLMode: "disable",
	})
	if err != nil {
		log.Error("init postgres", "error", err)
		return err
	}

	cache, err := redis.NewClient(ctx, &redis.Config{Addr: "localhost:6379"})
	if err != nil {
		pool.Close()
		log.Error("init redis", "error", err)
		return err
	}

	executor := exec.NewExecutor(pool)
	txManager := tx.NewManager(executor)
	// Repositories depend on exec.Querier (executor), use cases on tx.Manager.

	server := httpserver.New(httpserver.Config{Addr: ":8080"}, newRouter(executor, txManager, cache))

	// Components are stopped in reverse order: register connections first
	// so they are closed after everything that uses them.
	return app.New(log,
		postgres.NewPoolComponent(pool, log, 0),
		redis.NewClientComponent(cache, log, 0),
		server,
	).With(app.WithShutdownTimeout(10 * time.Second)).Run(ctx)
}
```

`postgres.Config` and `redis.Config` carry `yaml` tags, so they can be embedded into a
service config as is. `postgres.Config.DSN`, when set, is used instead of the separate fields.

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

### RabbitMQ

```go
conn, err := rabbitmq.Dial(ctx, &rabbitmq.Config{Host: "localhost", Port: 5672, Username: "guest", Password: "guest"})

// Delayed delivery: messages wait in "delay" and are dead-lettered into "main".
err = rabbitmq.Topology{
	Exchanges: []rabbitmq.Exchange{
		{Name: "main", Kind: amqp.ExchangeDirect, Durable: true},
		{Name: "delay", Kind: amqp.ExchangeDirect, Durable: true},
	},
	Queues: []rabbitmq.Queue{
		{Name: "main", Durable: true},
		{Name: "delay", Durable: true, Args: rabbitmq.DelayQueueArgs("main", "", 0)},
	},
	Bindings: []rabbitmq.Binding{
		{Queue: "main", Exchange: "main"},
		{Queue: "delay", Exchange: "delay"},
	},
}.Declare(conn)

publisher, err := rabbitmq.NewPublisher(conn)
err = publisher.Publish(ctx, "delay", "", amqp.Publishing{Body: body, Expiration: rabbitmq.Expiration(time.Minute)})
if errors.Is(err, rabbitmq.ErrUnroutable) {
	// no queue is bound for this exchange and routing key: the message was not stored
}

consumer := rabbitmq.NewConsumer(conn, rabbitmq.ConsumerConfig{Queue: "main", Prefetch: 10},
	func(ctx context.Context, d amqp.Delivery) error {
		return handle(ctx, d.Body) // nil acks; an error or a panic rejects without requeue
	}, log)

a := app.New(log,
	rabbitmq.NewConnectionComponent(conn), // fails the app if the broker drops the connection
	consumer,                              // finishes in-flight deliveries on shutdown
)
```

### HTTP server

`httpserver.Server` listens on `Config.Addr` and serves until `Shutdown` is called or the
context passed to `Start` is cancelled. Either way it stops accepting connections and waits
for active requests: `Shutdown` within its ctx deadline, a cancelled `Start` within
`Config.ShutdownTimeout` (10s by default). `ReadHeaderTimeout` defaults to 5s; other
timeouts are off unless set. `Addr()` returns the bound address, handy with `":0"` in tests.

### Logging

`log.New` builds a `*slog.Logger` (JSON or text) whose handler appends the fields stored in
the context to every record logged with a `*Context` method. `Inject` starts a mutable set of
fields; `Add` extends it further down the call chain, and the caller of `Inject` sees the
additions, so a request logger can report a user id that authentication found later:

```go
logger, err := log.New(log.Config{Level: "debug", Format: log.FormatText}, os.Stdout)

ctx = log.Inject(ctx, "request_id", requestID)
log.Add(ctx, "user_id", userID)
logger.InfoContext(ctx, "request handled")
```

`log.Wrap` attaches fields to an error, and `log.Err(err)` renders the full message together
with those fields as one `error` group. `log.Discard()` returns a logger for tests.

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

## Upgrading from v0.3

v0.4.0 replaces the zap-based `log` package with one built on `log/slog`; the rest is unchanged.

- `log.Default`, `log.Log`, `log.Logger`, `AsyncLogger`, `LogPanic` and the `LogGRPC` adapter are
  removed together with the zap and go-grpc-middleware dependencies. Build a `*slog.Logger` with
  `log.New(cfg, w)` and pass it explicitly; it satisfies `app.Logger`.
- `log.Config` keeps only `Level`, `Format` and `AddSource`. An invalid level or format is an
  error from `New` instead of a nil logger.
- Context fields are no longer read from string keys such as `_request_id`: store them with
  `log.Inject` and `log.Add`. The `_debug` context switch is gone; the level is the only filter.
- Importing the package no longer subscribes to `SIGHUP`.
- `FieldsError` gains `Unwrap`, so `errors.As` and `errors.Is` see through it; `Origin` is
  removed. `Wrap` no longer modifies the fields of the error it wraps.
- `ctxutil.Lookup` returns a context value without allocating when it is missing.
- Minimum dependency versions are raised to fix known vulnerabilities: `pgx` v5.11.0,
  `amqp091-go` v1.15.0, `golang.org/x/text` v0.42.0.

## Upgrading from v0.2

v0.3.0 rewrites the PostgreSQL part and raises the Go version; the rest is new.

- **Go 1.27** is required (v0.2 declared Go 1.23).
- **`postgres/uow` is removed.** Instead of `BeginWithTx` / `Commit` / `Rollback`, wrap the
  work in `tx.Manager.WithTransaction(ctx, fn)`: it commits when `fn` returns nil and rolls back
  on an error or a panic. Nested calls join the outer transaction.
- **`postgres/executor` is rewritten.** `executor.NewManager(pool)` becomes
  `executor.NewExecutor(pool)`; repositories call `GetExecutor(ctx)` as before and should depend
  on the narrow `executor.Querier`. `InjectTx` / `ExtractTx` became
  `InjectTxExecutor` / `GetTxExecutor`; batch executors are replaced by `ExecBatch(ctx, *pgx.Batch)`.
- **`postgres.NewPool(ctx, cfg)`** returns a plain `*pgxpool.Pool` and an error, checking the
  connection with a ping; `postgres.Pool` and `postgres.Connection` are removed. Register
  `postgres.NewPoolComponent` with `app` to close the pool on shutdown.
- **`postgres.Config`** gains `DSN`, `SSLMode` and pool settings; `Migration` is removed, since
  the library does not run migrations. `ConnectionString()` now returns an escaped
  `postgres://` URL instead of a key/value string.
- New packages: `app`, `cron`, `ctxutil`, `httpserver`, `postgres/tx`, `rabbitmq`, `redis`,
  `gowrap`. `kafka` and `log` are unchanged.

## Development

```sh
go test -race ./...
```

Integration tests of `postgres/tx`, `rabbitmq` and `redis` run only when the services are available:

```sh
docker run -d --rm --name pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16-alpine
docker run -d --rm --name rabbit -p 55672:5672 rabbitmq:3-alpine
docker run -d --rm --name redis -p 56379:6379 redis:7-alpine

PACKAGES_TEST_POSTGRES_DSN='postgres://postgres:test@localhost:55432/postgres?sslmode=disable' \
PACKAGES_TEST_RABBITMQ_URL='amqp://guest:guest@localhost:55672/' \
PACKAGES_TEST_REDIS_ADDR='localhost:56379' \
	go test -race ./...
```

Code generation requires [`ifacemaker`](https://github.com/vburenin/ifacemaker) and
[`gowrap`](https://github.com/hexdigest/gowrap) in `PATH`:

```sh
go generate ./...
```

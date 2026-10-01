package log

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
)

func decode(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()

	var records []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}

		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode %q: %v", line, err)
		}
		records = append(records, record)
	}

	return records
}

func TestNewRejectsInvalidConfig(t *testing.T) {
	t.Parallel()

	for name, cfg := range map[string]Config{
		"level":  {Level: "verbose"},
		"format": {Format: "xml"},
	} {
		if _, err := New(cfg, &bytes.Buffer{}); err == nil {
			t.Errorf("%s: expected an error", name)
		}
	}
}

func TestNewFiltersByLevel(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := New(Config{Level: "WARN"}, &buf)
	if err != nil {
		t.Fatal(err)
	}

	logger.Info("dropped")
	logger.Warn("kept")

	records := decode(t, &buf)
	if len(records) != 1 || records[0]["msg"] != "kept" {
		t.Fatalf("records = %v", records)
	}
}

func TestNewTextFormat(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := New(Config{Format: FormatText}, &buf)
	if err != nil {
		t.Fatal(err)
	}

	logger.InfoContext(Inject(context.Background(), "request_id", "r1"), "hello")

	if got := buf.String(); !strings.Contains(got, "msg=hello") || !strings.Contains(got, "request_id=r1") {
		t.Fatalf("output = %q", got)
	}
}

func TestContextFieldsReachRecords(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := New(Config{}, &buf)
	if err != nil {
		t.Fatal(err)
	}

	ctx := Inject(context.Background(), "request_id", "r1")
	Add(ctx, "user_id", "u1")

	logger.With("component", "grpc").InfoContext(ctx, "handled")
	logger.Info("no context")

	records := decode(t, &buf)
	if len(records) != 2 {
		t.Fatalf("records = %v", records)
	}

	first := records[0]
	if first["request_id"] != "r1" || first["user_id"] != "u1" || first["component"] != "grpc" {
		t.Fatalf("first record = %v", first)
	}

	if _, ok := records[1]["request_id"]; ok {
		t.Fatalf("second record has context fields: %v", records[1])
	}
}

func TestAddIsVisibleToTheInjectingCaller(t *testing.T) {
	t.Parallel()

	ctx := Inject(context.Background(), "request_id", "r1")

	inner, cancel := context.WithCancel(ctx)
	defer cancel()

	Add(inner, "user_id", "u1")

	got := fmt.Sprint(Fields(ctx))
	if got != "[request_id r1 user_id u1]" {
		t.Fatalf("fields = %s", got)
	}
}

func TestInjectInheritsWithoutSharing(t *testing.T) {
	t.Parallel()

	parent := Inject(context.Background(), "request_id", "r1")
	child := Inject(parent, "span", "s1")

	Add(child, "user_id", "u1")

	if got := fmt.Sprint(Fields(parent)); got != "[request_id r1]" {
		t.Fatalf("parent fields = %s", got)
	}

	if got := fmt.Sprint(Fields(child)); got != "[request_id r1 span s1 user_id u1]" {
		t.Fatalf("child fields = %s", got)
	}
}

func TestAddWithoutInjectIsNoop(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	Add(ctx, "user_id", "u1")

	if fields := Fields(ctx); fields != nil {
		t.Fatalf("fields = %v", fields)
	}
}

func TestAddIsSafeForConcurrentUse(t *testing.T) {
	t.Parallel()

	ctx := Inject(context.Background())
	logger := slog.New(NewContextHandler(slog.DiscardHandler))

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			Add(ctx, "n", i)
			logger.InfoContext(ctx, "tick")
		})
	}
	wg.Wait()

	if got := len(Fields(ctx)); got != 100 {
		t.Fatalf("len(fields) = %d", got)
	}
}

var errNotFound = errors.New("not found")

func TestWrapMergesFieldsAndKeepsChain(t *testing.T) {
	t.Parallel()

	inner := Wrap("load user", errNotFound, Fld{"user_id": "u1", "attempt": 1})
	outer := Wrap("handle request", inner, Fld{"attempt": 2})

	if !errors.Is(outer, errNotFound) {
		t.Fatal("errors.Is lost the original error")
	}

	if got := outer.Error(); got != "handle request: load user: not found" {
		t.Fatalf("message = %q", got)
	}

	var fieldsErr *FieldsError
	if !errors.As(outer, &fieldsErr) {
		t.Fatal("errors.As found no FieldsError")
	}

	fields := fieldsErr.Fields()
	if fields["user_id"] != "u1" || fields["attempt"] != 2 {
		t.Fatalf("fields = %v", fields)
	}

	if inner.(*FieldsError).fields["attempt"] != 1 {
		t.Fatal("Wrap modified the fields of the inner error")
	}
}

func TestErrAttribute(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	logger, err := New(Config{}, &buf)
	if err != nil {
		t.Fatal(err)
	}

	wrapped := fmt.Errorf("outer: %w", Wrap("load user", errNotFound, Fld{"user_id": "u1"}))

	logger.Error("plain", Err(errNotFound))
	logger.Error("with fields", Err(wrapped))
	logger.Error("nil", Err(nil))

	records := decode(t, &buf)
	if len(records) != 3 {
		t.Fatalf("records = %v", records)
	}

	if records[0][ErrorKey] != "not found" {
		t.Fatalf("plain = %v", records[0])
	}

	group, ok := records[1][ErrorKey].(map[string]any)
	if !ok || group["msg"] != "outer: load user: not found" || group["user_id"] != "u1" {
		t.Fatalf("with fields = %v", records[1])
	}

	if _, ok := records[2][ErrorKey]; ok {
		t.Fatalf("nil = %v", records[2])
	}
}

func TestFieldsWithoutInjectDoesNotAllocate(t *testing.T) {
	ctx := context.Background()

	if allocs := testing.AllocsPerRun(100, func() { _ = Fields(ctx) }); allocs != 0 {
		t.Fatalf("Fields() without Inject allocates %v times, want 0", allocs)
	}
}

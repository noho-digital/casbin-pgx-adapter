package pgxadapter

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// safeToRetryError is an error that pgconn.SafeToRetry returns true for.
type safeToRetryError struct {
	msg string
}

func (e *safeToRetryError) Error() string        { return e.msg }
func (e *safeToRetryError) SafeToRetry() bool     { return true }
func (e *safeToRetryError) Unwrap() error         { return nil }

// unsafeError is an error that pgconn.SafeToRetry returns false for.
type unsafeError struct {
	msg string
}

func (e *unsafeError) Error() string { return e.msg }

// mockDB implements the DB interface for testing retry logic.
type mockDB struct {
	execCalls  atomic.Int32
	queryCalls atomic.Int32
	beginCalls atomic.Int32

	// Number of times to fail before succeeding.
	failCount int
	// Error to return on failure. Must satisfy pgconn.SafeToRetry for retry tests.
	failErr error
}

func (m *mockDB) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	n := int(m.execCalls.Add(1))
	if n <= m.failCount {
		return pgconn.CommandTag{}, m.failErr
	}
	return pgconn.NewCommandTag("INSERT 0 1"), nil
}

func (m *mockDB) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	n := int(m.queryCalls.Add(1))
	if n <= m.failCount {
		return nil, m.failErr
	}
	// Return nil rows — callers in retry tests only check the error.
	return nil, nil
}

func (m *mockDB) Begin(ctx context.Context) (pgx.Tx, error) {
	n := int(m.beginCalls.Add(1))
	if n <= m.failCount {
		return nil, m.failErr
	}
	return nil, nil
}

func newTestAdapter(db DB, maxRetries int) *PgxAdapter {
	return &PgxAdapter{
		db:         db,
		tableName:  defaultTableName,
		database:   defaultDatabase,
		psql:       sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
		maxRetries: maxRetries,
	}
}

func TestExecWithRetry_succeeds_on_first_try(t *testing.T) {
	t.Parallel()
	mock := &mockDB{failCount: 0}
	a := newTestAdapter(mock, defaultMaxRetries)

	result, err := a.execWithRetry(context.Background(), "INSERT INTO test VALUES ($1)", "val")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("expected 1 row affected, got %d", result.RowsAffected())
	}
	if mock.execCalls.Load() != 1 {
		t.Fatalf("expected 1 call, got %d", mock.execCalls.Load())
	}
}

func TestExecWithRetry_retries_on_safe_error(t *testing.T) {
	t.Parallel()
	mock := &mockDB{
		failCount: 1,
		failErr:   &safeToRetryError{msg: "connection reset"},
	}
	a := newTestAdapter(mock, defaultMaxRetries)

	result, err := a.execWithRetry(context.Background(), "INSERT INTO test VALUES ($1)", "val")
	if err != nil {
		t.Fatalf("expected no error after retry, got %v", err)
	}
	if result.RowsAffected() != 1 {
		t.Fatalf("expected 1 row affected, got %d", result.RowsAffected())
	}
	if mock.execCalls.Load() != 2 {
		t.Fatalf("expected 2 calls (1 fail + 1 success), got %d", mock.execCalls.Load())
	}
}

func TestExecWithRetry_no_retry_on_unsafe_error(t *testing.T) {
	t.Parallel()
	mock := &mockDB{
		failCount: 1,
		failErr:   &unsafeError{msg: "syntax error"},
	}
	a := newTestAdapter(mock, defaultMaxRetries)

	_, err := a.execWithRetry(context.Background(), "INSERT INTO test VALUES ($1)", "val")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if mock.execCalls.Load() != 1 {
		t.Fatalf("expected 1 call (no retry for unsafe error), got %d", mock.execCalls.Load())
	}
}

func TestExecWithRetry_exhausts_retries(t *testing.T) {
	t.Parallel()
	mock := &mockDB{
		failCount: 10,
		failErr:   &safeToRetryError{msg: "connection reset"},
	}
	a := newTestAdapter(mock, defaultMaxRetries)

	_, err := a.execWithRetry(context.Background(), "INSERT INTO test VALUES ($1)", "val")
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	// 1 initial + 2 retries = 3
	if mock.execCalls.Load() != 3 {
		t.Fatalf("expected 3 calls (1 initial + 2 retries), got %d", mock.execCalls.Load())
	}
}

func TestQueryWithRetry_retries_on_safe_error(t *testing.T) {
	t.Parallel()
	mock := &mockDB{
		failCount: 1,
		failErr:   &safeToRetryError{msg: "i/o timeout"},
	}
	a := newTestAdapter(mock, defaultMaxRetries)

	_, err := a.queryWithRetry(context.Background(), "SELECT * FROM test")
	if err != nil {
		t.Fatalf("expected no error after retry, got %v", err)
	}
	if mock.queryCalls.Load() != 2 {
		t.Fatalf("expected 2 calls, got %d", mock.queryCalls.Load())
	}
}

func TestBeginWithRetry_retries_on_safe_error(t *testing.T) {
	t.Parallel()
	mock := &mockDB{
		failCount: 1,
		failErr:   &safeToRetryError{msg: "dial tcp timeout"},
	}
	a := newTestAdapter(mock, defaultMaxRetries)

	_, err := a.beginWithRetry(context.Background())
	if err != nil {
		t.Fatalf("expected no error after retry, got %v", err)
	}
	if mock.beginCalls.Load() != 2 {
		t.Fatalf("expected 2 calls, got %d", mock.beginCalls.Load())
	}
}

func TestAddPolicyCtx_retries_on_connection_error(t *testing.T) {
	t.Parallel()
	mock := &mockDB{
		failCount: 1,
		failErr:   &safeToRetryError{msg: "connection refused"},
	}
	a := newTestAdapter(mock, defaultMaxRetries)

	err := a.AddPolicyCtx(context.Background(), "p", "p", []string{"alice", "data1", "read"})
	if err != nil {
		t.Fatalf("expected no error after retry, got %v", err)
	}
	if mock.execCalls.Load() != 2 {
		t.Fatalf("expected 2 exec calls, got %d", mock.execCalls.Load())
	}
}

func TestWithMaxRetries_option(t *testing.T) {
	t.Parallel()
	mock := &mockDB{
		failCount: 10,
		failErr:   &safeToRetryError{msg: "connection reset"},
	}
	a := newTestAdapter(mock, 0)
	WithMaxRetries(5)(a)

	_, err := a.execWithRetry(context.Background(), "INSERT INTO test VALUES ($1)", "val")
	if err == nil {
		t.Fatal("expected error after exhausting retries, got nil")
	}
	// 1 initial + 5 retries = 6
	expected := int32(6)
	if mock.execCalls.Load() != expected {
		t.Fatalf("expected %d calls, got %d", expected, mock.execCalls.Load())
	}
}

func TestSafeToRetryErrorInterface(t *testing.T) {
	t.Parallel()
	safeErr := &safeToRetryError{msg: "test"}
	if !pgconn.SafeToRetry(safeErr) {
		t.Fatal("expected SafeToRetry to return true")
	}

	unsafeErr := &unsafeError{msg: "test"}
	if pgconn.SafeToRetry(unsafeErr) {
		t.Fatal("expected SafeToRetry to return false")
	}

	plainErr := fmt.Errorf("plain error")
	if pgconn.SafeToRetry(plainErr) {
		t.Fatal("expected SafeToRetry to return false for plain error")
	}
}

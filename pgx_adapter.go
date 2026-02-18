package pgxadapter

import (
	"context"
	"fmt"
	"strings"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/casbin/casbin/v3/persist"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var _ persist.Adapter = (*PgxAdapter)(nil)

// DB represents database operations needed by the adapter.
// Both *pgx.Conn and *pgxpool.Pool implement this interface.
type DB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	Begin(ctx context.Context) (pgx.Tx, error)
}

const (
	defaultTableName = "casbin_rule"
	defaultDatabase  = "casbin"
)

// PgxAdapter represents the pgx adapter for policy persistence
type PgxAdapter struct {
	db         DB
	conn       *pgx.Conn
	pool       *pgxpool.Pool
	tableName  string
	database   string
	psql       sq.StatementBuilderType
	isFiltered bool
	indexes    [][]string
	mu         sync.RWMutex

	// pool configuration
	usePool bool
}

// Option is a function that configures the adapter
type Option func(*PgxAdapter)

// WithTableName sets a custom table name for the adapter
func WithTableName(tableName string) Option {
	return func(a *PgxAdapter) {
		a.tableName = tableName
	}
}

// WithDatabaseName sets a custom database name for the adapter
func WithDatabaseName(database string) Option {
	return func(a *PgxAdapter) {
		a.database = database
	}
}

// WithIndex adds a composite index on the specified columns.
// Valid columns are: ptype, v0, v1, v2, v3, v4, v5.
// Can be called multiple times to add multiple indexes.
func WithIndex(columns ...string) Option {
	return func(a *PgxAdapter) {
		if len(columns) > 0 {
			a.indexes = append(a.indexes, columns)
		}
	}
}

// WithPool configures the adapter to use a connection pool instead of a single connection.
// Pool settings can be configured via connection string parameters (e.g., pool_max_conns, pool_min_conns).
func WithPool() Option {
	return func(a *PgxAdapter) {
		a.usePool = true
	}
}

// NewAdapter creates a new adapter with a connection string.
// If WithPool is provided, a connection pool is created. Otherwise, a single connection is used.
func NewAdapter(connStr string, opts ...Option) (*PgxAdapter, error) {
	ctx := context.Background()

	// Apply options to determine if we should use a pool
	var cfg PgxAdapter
	for _, opt := range opts {
		opt(&cfg)
	}

	if cfg.usePool {
		pool, err := pgxpool.New(ctx, connStr)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pool: %w", err)
		}

		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, fmt.Errorf("failed to ping database: %w", err)
		}

		return NewAdapterWithPool(pool, opts...)
	}

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return NewAdapterWithConn(conn, opts...)
}

// NewAdapterWithConn creates a new adapter with an existing connection
func NewAdapterWithConn(conn *pgx.Conn, opts ...Option) (*PgxAdapter, error) {
	a := &PgxAdapter{
		db:        conn,
		conn:      conn,
		tableName: defaultTableName,
		database:  defaultDatabase,
		psql:      sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}

	// Apply options
	for _, opt := range opts {
		opt(a)
	}

	// Create table if it doesn't exist
	if err := a.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return a, nil
}

// NewAdapterWithPool creates a new adapter with an existing connection pool
func NewAdapterWithPool(pool *pgxpool.Pool, opts ...Option) (*PgxAdapter, error) {
	a := &PgxAdapter{
		db:        pool,
		pool:      pool,
		tableName: defaultTableName,
		database:  defaultDatabase,
		psql:      sq.StatementBuilder.PlaceholderFormat(sq.Dollar),
	}

	// Apply options
	for _, opt := range opts {
		opt(a)
	}

	// Create table if it doesn't exist
	if err := a.createTable(); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return a, nil
}

// createTable creates the casbin_rule table if it doesn't exist
func (a *PgxAdapter) createTable() error {
	ctx := context.Background()

	// Use pgx identifier quoting for secure table name handling
	quotedTableName := pgx.Identifier{a.tableName}.Sanitize()
	quotedIndexName := pgx.Identifier{"idx_" + a.tableName}.Sanitize()

	createTableSQL := `CREATE TABLE IF NOT EXISTS ` + quotedTableName + ` (
		id SERIAL PRIMARY KEY,
		ptype VARCHAR(100) NOT NULL,
		v0 VARCHAR(100),
		v1 VARCHAR(100),
		v2 VARCHAR(100),
		v3 VARCHAR(100),
		v4 VARCHAR(100),
		v5 VARCHAR(100)
	)`

	createIndexSQL := `CREATE UNIQUE INDEX IF NOT EXISTS ` + quotedIndexName + `
		ON ` + quotedTableName + `(ptype, COALESCE(v0,''), COALESCE(v1,''), COALESCE(v2,''), COALESCE(v3,''), COALESCE(v4,''), COALESCE(v5,''))`

	// Execute creation statements
	if _, err := a.db.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	if _, err := a.db.Exec(ctx, createIndexSQL); err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	// Create custom indexes
	for _, columns := range a.indexes {
		if err := a.createIndex(ctx, columns); err != nil {
			return err
		}
	}

	return nil
}

func (a *PgxAdapter) createIndex(ctx context.Context, columns []string) error {
	quotedTableName := pgx.Identifier{a.tableName}.Sanitize()
	indexName := "idx_" + a.tableName + "_" + strings.Join(columns, "_")
	quotedIndexName := pgx.Identifier{indexName}.Sanitize()

	var quotedColumns []string
	for _, col := range columns {
		quotedColumns = append(quotedColumns, pgx.Identifier{col}.Sanitize())
	}

	createIndexSQL := `CREATE INDEX IF NOT EXISTS ` + quotedIndexName +
		` ON ` + quotedTableName + `(` + strings.Join(quotedColumns, ", ") + `)`

	if _, err := a.db.Exec(ctx, createIndexSQL); err != nil {
		return fmt.Errorf("failed to create index %s: %w", indexName, err)
	}

	return nil
}

// GetConn returns the underlying database connection.
// Returns nil if the adapter was created with a pool.
func (a *PgxAdapter) GetConn() *pgx.Conn {
	return a.conn
}

// GetPool returns the underlying connection pool.
// Returns nil if the adapter was created with a single connection.
func (a *PgxAdapter) GetPool() *pgxpool.Pool {
	return a.pool
}

// GetDB returns the underlying database interface.
// This can be used for custom queries and works with both single connections and pools.
func (a *PgxAdapter) GetDB() DB {
	return a.db
}

// GetTableName returns the table name used by the adapter
func (a *PgxAdapter) GetTableName() string {
	return a.tableName
}

// GetDatabase returns the database name used by the adapter
func (a *PgxAdapter) GetDatabase() string {
	return a.database
}

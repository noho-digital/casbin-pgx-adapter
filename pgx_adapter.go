package pgxadapter

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultTableName = "casbin_rule"
	defaultDatabase  = "casbin"
)

// PgxAdapter represents the pgx adapter for policy persistence
type PgxAdapter struct {
	pool      *pgxpool.Pool
	conn      *pgx.Conn
	tableName string
	database  string
	psql      sq.StatementBuilderType
}

// Option is a function that configures the adapter
type Option func(*PgxAdapter)

// WithTableName sets a custom table name for the adapter
func WithTableName(tableName string) Option {
	return func(a *PgxAdapter) {
		a.tableName = tableName
	}
}

// WithDatabase sets a custom database name for the adapter
func WithDatabase(database string) Option {
	return func(a *PgxAdapter) {
		a.database = database
	}
}

// NewAdapter creates a new adapter with a connection string
func NewAdapter(connStr string, opts ...Option) (*PgxAdapter, error) {
	ctx := context.Background()

	// Parse and connect to the database
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return NewAdapterWithPool(pool, opts...)
}

// NewAdapterWithPool creates a new adapter with an existing connection pool
func NewAdapterWithPool(pool *pgxpool.Pool, opts ...Option) (*PgxAdapter, error) {
	a := &PgxAdapter{
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

// NewAdapterWithConn creates a new adapter with an existing connection
func NewAdapterWithConn(conn *pgx.Conn, opts ...Option) (*PgxAdapter, error) {
	a := &PgxAdapter{
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
	var err error
	if a.pool != nil {
		_, err = a.pool.Exec(ctx, createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		_, err = a.pool.Exec(ctx, createIndexSQL)
	} else if a.conn != nil {
		_, err = a.conn.Exec(ctx, createTableSQL)
		if err != nil {
			return fmt.Errorf("failed to create table: %w", err)
		}
		_, err = a.conn.Exec(ctx, createIndexSQL)
	}

	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return nil
}

// beginTx starts a transaction based on the connection type
func (a *PgxAdapter) beginTx(ctx context.Context) (pgx.Tx, error) {
	if a.pool != nil {
		return a.pool.Begin(ctx)
	}
	if a.conn != nil {
		return a.conn.Begin(ctx)
	}
	return nil, fmt.Errorf("no database connection available")
}

// Close closes the database connection
func (a *PgxAdapter) Close() {
	if a.pool != nil {
		a.pool.Close()
	}
	// Note: conn is not closed as it may be managed externally
}

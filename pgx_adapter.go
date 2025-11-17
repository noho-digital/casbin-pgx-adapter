package pgxadapter

import (
	"context"
	"fmt"
	"sync"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
)

const (
	defaultTableName = "casbin_rule"
	defaultDatabase  = "casbin"
)

// PgxAdapter represents the pgx adapter for policy persistence
type PgxAdapter struct {
	conn       *pgx.Conn
	tableName  string
	database   string
	psql       sq.StatementBuilderType
	isFiltered bool
	mu         sync.RWMutex
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

// NewAdapter creates a new adapter with a connection string
func NewAdapter(connStr string, opts ...Option) (*PgxAdapter, error) {
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test the connection
	if err := conn.Ping(ctx); err != nil {
		conn.Close(ctx)
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return NewAdapterWithConn(conn, opts...)
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
	if _, err := a.conn.Exec(ctx, createTableSQL); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}
	if _, err := a.conn.Exec(ctx, createIndexSQL); err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}

	return nil
}

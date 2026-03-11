package pgxadapter_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxadapter "github.com/noho-digital/casbin-pgx-adapter"
)

// testPsql is a squirrel statement builder with PostgreSQL dollar placeholders for use in tests.
var testPsql = sq.StatementBuilder.PlaceholderFormat(sq.Dollar)

// TestModelText is a shared Casbin model configuration used across multiple tests
var TestModelText = `
[request_definition]
r = sub, obj, act

[policy_definition]
p = sub, obj, act

[role_definition]
g = _, _

[policy_effect]
e = some(where (p.eft == allow))

[matchers]
m = g(r.sub, p.sub) && r.obj == p.obj && r.act == p.act
`

func getTestDBURL() string {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
	}
	return dbURL
}

func TestNewAdapter(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		usePool   bool
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "successful_creation_with_default_table",
			tableName: "casbin_test_new_adapter",
			usePool:   false,
			wantErr:   false,
		},
		{
			name:      "successful_creation_with_custom_table",
			tableName: "custom_casbin_table",
			usePool:   false,
			wantErr:   false,
		},
		{
			name:      "successful_creation_with_pool",
			tableName: "casbin_test_with_pool",
			usePool:   true,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dbURL := getTestDBURL()

			// Clean up table before test
			cleanupConn, err := pgx.Connect(context.Background(), dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}
			quotedTableName := pgx.Identifier{tt.tableName}.Sanitize()
			_, _ = cleanupConn.Exec(context.Background(), "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			cleanupConn.Close(context.Background())

			opts := []pgxadapter.Option{pgxadapter.WithTableName(tt.tableName)}
			if tt.usePool {
				opts = append(opts, pgxadapter.WithPool())
			}

			adapter, err := pgxadapter.NewAdapter(dbURL, opts...)

			if tt.wantErr {
				if err == nil {
					t.Errorf("pgxadapter.NewAdapter() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("pgxadapter.NewAdapter() unexpected error: %v", err)
			}

			t.Cleanup(func() {
				if adapter != nil {
					ctx := context.Background()
					// Use a separate connection for cleanup
					cleanConn, err := pgx.Connect(ctx, dbURL)
					if err == nil {
						_, _ = cleanConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
						cleanConn.Close(ctx)
					}
					if adapter.GetPool() != nil {
						adapter.GetPool().Close()
					}
					adapter.GetDB().Close()
				}
			})

			if adapter.GetTableName() != tt.tableName {
				t.Errorf("pgxadapter.NewAdapter() tableName = %v, want %v", adapter.GetTableName(), tt.tableName)
			}

			if tt.usePool {
				if adapter.GetPool() == nil {
					t.Error("pgxadapter.NewAdapter() with WithPool() expected pool to be set")
				}
			}

			if adapter.GetDB() == nil {
				t.Error("pgxadapter.NewAdapter() expected db to be set")
			}
		})
	}
}

func TestNewAdapterWithConfig(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{
			name:      "successful_creation_with_default_table",
			tableName: "casbin_test_with_config",
			wantErr:   false,
		},
		{
			name:      "successful_creation_with_custom_table",
			tableName: "custom_config_table",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbURL := getTestDBURL()

			config, err := pgx.ParseConfig(dbURL)
			if err != nil {
				t.Skipf("Could not parse connection string: %v", err)
			}

			// Clean up any existing test table
			quotedTableName := pgx.Identifier{tt.tableName}.Sanitize()
			cleanupConn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}
			_, _ = cleanupConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			cleanupConn.Close(ctx)

			t.Cleanup(func() {
				cleanConn, err := pgx.Connect(ctx, dbURL)
				if err == nil {
					_, _ = cleanConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
					cleanConn.Close(ctx)
				}
			})

			adapter, err := pgxadapter.NewAdapterWithConfig(config, pgxadapter.WithTableName(tt.tableName))

			if tt.wantErr {
				if err == nil {
					t.Errorf("pgxadapter.NewAdapterWithConfig() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("pgxadapter.NewAdapterWithConfig() unexpected error: %v", err)
			}

			t.Cleanup(func() {
				if adapter != nil {
					adapter.GetDB().Close()
				}
			})

			if adapter.GetTableName() != tt.tableName {
				t.Errorf("pgxadapter.NewAdapterWithConfig() tableName = %v, want %v", adapter.GetTableName(), tt.tableName)
			}

			if adapter.GetDB() == nil {
				t.Error("pgxadapter.NewAdapterWithConfig() expected db to be set")
			}

			if adapter.GetPool() != nil {
				t.Error("pgxadapter.NewAdapterWithConfig() expected pool to be nil (single connection)")
			}
		})
	}
}

func TestNewAdapterWithConn(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{
			name:      "successful_creation_with_conn",
			tableName: "casbin_test_with_conn",
			wantErr:   false,
		},
		{
			name:      "successful_creation_with_custom_table",
			tableName: "custom_conn_table",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbURL := getTestDBURL()

			conn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}

			if err := conn.Ping(ctx); err != nil {
				conn.Close(ctx)
				t.Skipf("Could not ping test database: %v", err)
			}

			// Clean up any existing test table
			quotedTableName := pgx.Identifier{tt.tableName}.Sanitize()
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")

			t.Cleanup(func() {
				cleanConn, err := pgx.Connect(ctx, dbURL)
				if err == nil {
					_, _ = cleanConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
					cleanConn.Close(ctx)
				}
			})

			// NewAdapterWithConn closes the passed conn after extracting config
			adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tt.tableName))

			if tt.wantErr {
				if err == nil {
					t.Errorf("pgxadapter.NewAdapterWithConn() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("pgxadapter.NewAdapterWithConn() unexpected error: %v", err)
			}

			if adapter.GetTableName() != tt.tableName {
				t.Errorf("pgxadapter.NewAdapterWithConn() tableName = %v, want %v", adapter.GetTableName(), tt.tableName)
			}

			if adapter.GetDB() == nil {
				t.Error("pgxadapter.NewAdapterWithConn() expected db to be set")
			}
		})
	}
}

func TestNewAdapterWithPool(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		wantErr   bool
	}{
		{
			name:      "successful_creation_with_pool",
			tableName: "casbin_test_with_pool_conn",
			wantErr:   false,
		},
		{
			name:      "successful_creation_with_custom_table",
			tableName: "custom_pool_table",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbURL := getTestDBURL()

			pool, err := pgxpool.New(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not create pool for test database: %v", err)
			}

			if err := pool.Ping(ctx); err != nil {
				pool.Close()
				t.Skipf("Could not ping test database: %v", err)
			}

			// Clean up any existing test table
			quotedTableName := pgx.Identifier{tt.tableName}.Sanitize()
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")

			t.Cleanup(func() {
				_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
				pool.Close()
			})

			adapter, err := pgxadapter.NewAdapterWithPool(pool, pgxadapter.WithTableName(tt.tableName))

			if tt.wantErr {
				if err == nil {
					t.Errorf("pgxadapter.NewAdapterWithPool() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("pgxadapter.NewAdapterWithPool() unexpected error: %v", err)
			}

			if adapter.GetTableName() != tt.tableName {
				t.Errorf("pgxadapter.NewAdapterWithPool() tableName = %v, want %v", adapter.GetTableName(), tt.tableName)
			}

			if adapter.GetPool() == nil {
				t.Error("pgxadapter.NewAdapterWithPool() expected pool to be set")
			}

			if adapter.GetDB() == nil {
				t.Error("pgxadapter.NewAdapterWithPool() expected db to be set")
			}
		})
	}
}

func TestGetDB(t *testing.T) {
	t.Run("returns_db_for_conn_adapter", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		dbURL := getTestDBURL()

		conn, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			t.Skipf("Could not connect to test database: %v", err)
		}

		tableName := "test_getdb_conn"
		quotedTableName := pgx.Identifier{tableName}.Sanitize()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")

		t.Cleanup(func() {
			cleanConn, err := pgx.Connect(ctx, dbURL)
			if err == nil {
				_, _ = cleanConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
				cleanConn.Close(ctx)
			}
		})

		adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
		if err != nil {
			t.Fatalf("Failed to create adapter: %v", err)
		}

		if adapter.GetDB() == nil {
			t.Error("pgxadapter.GetDB() expected db to be set for conn adapter")
		}
	})

	t.Run("returns_db_for_pool_adapter", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		dbURL := getTestDBURL()

		pool, err := pgxpool.New(ctx, dbURL)
		if err != nil {
			t.Skipf("Could not create pool: %v", err)
		}

		tableName := "test_getdb_pool"
		quotedTableName := pgx.Identifier{tableName}.Sanitize()
		_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")

		t.Cleanup(func() {
			_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			pool.Close()
		})

		adapter, err := pgxadapter.NewAdapterWithPool(pool, pgxadapter.WithTableName(tableName))
		if err != nil {
			t.Fatalf("Failed to create adapter: %v", err)
		}

		if adapter.GetDB() == nil {
			t.Error("pgxadapter.GetDB() expected db to be set for pool adapter")
		}
	})
}

func TestWithTableName(t *testing.T) {
	tests := []struct {
		name          string
		tableName     string
		expectedTable string
	}{
		{
			name:          "set_custom_table_name",
			tableName:     "my_custom_table",
			expectedTable: "my_custom_table",
		},
		{
			name:          "set_empty_table_name",
			tableName:     "",
			expectedTable: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbURL := getTestDBURL()

			conn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}

			testTableName := fmt.Sprintf("test_with_table_name_%s", tt.name)
			quotedTableName := pgx.Identifier{testTableName}.Sanitize()
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			conn.Close(ctx)

			t.Cleanup(func() {
				cleanConn, err := pgx.Connect(ctx, dbURL)
				if err == nil {
					_, _ = cleanConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
					cleanConn.Close(ctx)
				}
			})

			adapterConn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}

			// Create adapter with the test table name option
			adapter, err := pgxadapter.NewAdapterWithConn(adapterConn, pgxadapter.WithTableName(tt.tableName))
			if err != nil && tt.tableName != "" {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			if tt.tableName != "" && adapter.GetTableName() != tt.expectedTable {
				t.Errorf("pgxadapter.WithTableName() set tableName = %v, want %v", adapter.GetTableName(), tt.expectedTable)
			}
		})
	}
}

func TestWithDatabaseName(t *testing.T) {
	tests := []struct {
		name             string
		database         string
		expectedDatabase string
	}{
		{
			name:             "set_custom_database_name",
			database:         "my_custom_db",
			expectedDatabase: "my_custom_db",
		},
		{
			name:             "set_empty_database_name",
			database:         "",
			expectedDatabase: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbURL := getTestDBURL()

			conn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}

			testTableName := fmt.Sprintf("test_with_db_name_%s", tt.name)
			quotedTableName := pgx.Identifier{testTableName}.Sanitize()
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			conn.Close(ctx)

			t.Cleanup(func() {
				cleanConn, err := pgx.Connect(ctx, dbURL)
				if err == nil {
					_, _ = cleanConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
					cleanConn.Close(ctx)
				}
			})

			adapterConn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}

			// Create adapter with the test database name option
			adapter, err := pgxadapter.NewAdapterWithConn(adapterConn, pgxadapter.WithTableName(testTableName), pgxadapter.WithDatabaseName(tt.database))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			if adapter.GetDatabase() != tt.expectedDatabase {
				t.Errorf("pgxadapter.WithDatabaseName() set database = %v, want %v", adapter.GetDatabase(), tt.expectedDatabase)
			}
		})
	}
}

func TestWithIndex(t *testing.T) {
	tests := []struct {
		name            string
		indexes         [][]string
		expectedIndexes []string
	}{
		{
			name: "single_column_index",
			indexes: [][]string{
				{"v0"},
			},
			expectedIndexes: []string{"idx_test_with_index_single_column_index_v0"},
		},
		{
			name: "composite_index",
			indexes: [][]string{
				{"v0", "v1"},
			},
			expectedIndexes: []string{"idx_test_with_index_composite_index_v0_v1"},
		},
		{
			name: "multiple_indexes",
			indexes: [][]string{
				{"v0"},
				{"v1", "v2"},
				{"ptype", "v0", "v1"},
			},
			expectedIndexes: []string{
				"idx_test_with_index_multiple_indexes_v0",
				"idx_test_with_index_multiple_indexes_v1_v2",
				"idx_test_with_index_multiple_indexes_ptype_v0_v1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			dbURL := getTestDBURL()

			conn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}

			testTableName := fmt.Sprintf("test_with_index_%s", tt.name)
			quotedTableName := pgx.Identifier{testTableName}.Sanitize()
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			conn.Close(ctx)

			t.Cleanup(func() {
				cleanConn, err := pgx.Connect(ctx, dbURL)
				if err == nil {
					_, _ = cleanConn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
					cleanConn.Close(ctx)
				}
			})

			adapterConn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}

			opts := []pgxadapter.Option{pgxadapter.WithTableName(testTableName)}
			for _, idx := range tt.indexes {
				opts = append(opts, pgxadapter.WithIndex(idx...))
			}

			adapter, err := pgxadapter.NewAdapterWithConn(adapterConn, opts...)
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			// Use the adapter's *sql.DB for verification
			for _, expectedIndex := range tt.expectedIndexes {
				var exists bool
				err = adapter.GetDB().QueryRowContext(ctx,
					"SELECT EXISTS(SELECT 1 FROM pg_indexes WHERE tablename = $1 AND indexname = $2)",
					testTableName, expectedIndex).Scan(&exists)
				if err != nil {
					t.Fatalf("Failed to query index existence: %v", err)
				}
				if !exists {
					t.Errorf("Expected index %s to exist on table %s", expectedIndex, testTableName)
				}
			}
		})
	}
}

// setupTestAdapter creates a test adapter and returns it along with a *sql.DB for verification queries.
// The returned *sql.DB is the adapter's own database connection.
func setupTestAdapter(t *testing.T, tableName string) (*pgxadapter.PgxAdapter, *sql.DB) {
	t.Helper()

	ctx := context.Background()
	dbURL := getTestDBURL()

	// Use a pool-based adapter for tests since NewAdapterWithConn closes the conn
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Skipf("Could not create pool for test database: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("Could not ping test database: %v", err)
	}

	// Clean up any existing test table
	quotedTableName := pgx.Identifier{tableName}.Sanitize()
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")

	adapter, err := pgxadapter.NewAdapterWithPool(pool, pgxadapter.WithTableName(tableName))
	if err != nil {
		pool.Close()
		t.Fatalf("Failed to create adapter: %v", err)
	}

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
		pool.Close()
	})

	return adapter, adapter.GetDB()
}

package pgxadapter_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgxadapter "github.com/noho-digital/casbin-pgx-adapter"
)

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

			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
			}

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
					quotedTableName := pgx.Identifier{tt.tableName}.Sanitize()
					if adapter.GetPool() != nil {
						_, _ = adapter.GetPool().Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
						adapter.GetPool().Close()
					} else if adapter.GetConn() != nil {
						_, _ = adapter.GetConn().Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
						adapter.GetConn().Close(ctx)
					}
				}
			})

			if adapter.GetTableName() != tt.tableName {
				t.Errorf("pgxadapter.NewAdapter() tableName = %v, want %v", adapter.GetTableName(), tt.tableName)
			}

			if tt.usePool {
				if adapter.GetPool() == nil {
					t.Error("pgxadapter.NewAdapter() with WithPool() expected pool to be set")
				}
				if adapter.GetConn() != nil {
					t.Error("pgxadapter.NewAdapter() with WithPool() expected conn to be nil")
				}
			} else {
				if adapter.GetConn() == nil {
					t.Error("pgxadapter.NewAdapter() expected conn to be set")
				}
				if adapter.GetPool() != nil {
					t.Error("pgxadapter.NewAdapter() expected pool to be nil")
				}
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
			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
			}

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
				_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
				conn.Close(ctx)
			})

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

			if adapter.GetConn() == nil {
				t.Error("pgxadapter.NewAdapterWithConn() expected conn to be set")
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
			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
			}

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

			if adapter.GetConn() != nil {
				t.Error("pgxadapter.NewAdapterWithPool() expected conn to be nil")
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
		dbURL := os.Getenv("TEST_DATABASE_URL")
		if dbURL == "" {
			dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
		}

		conn, err := pgx.Connect(ctx, dbURL)
		if err != nil {
			t.Skipf("Could not connect to test database: %v", err)
		}

		tableName := "test_getdb_conn"
		quotedTableName := pgx.Identifier{tableName}.Sanitize()
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")

		t.Cleanup(func() {
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			conn.Close(ctx)
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
		dbURL := os.Getenv("TEST_DATABASE_URL")
		if dbURL == "" {
			dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
		}

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
			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
			}

			conn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}
			defer conn.Close(ctx)

			testTableName := fmt.Sprintf("test_with_table_name_%s", tt.name)
			quotedTableName := pgx.Identifier{testTableName}.Sanitize()
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			defer func() {
				_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			}()

			// Create adapter with the test table name option
			adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tt.tableName))
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
			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
			}

			conn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}
			defer conn.Close(ctx)

			testTableName := fmt.Sprintf("test_with_db_name_%s", tt.name)
			quotedTableName := pgx.Identifier{testTableName}.Sanitize()
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			defer func() {
				_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			}()

			// Create adapter with the test database name option
			adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(testTableName), pgxadapter.WithDatabaseName(tt.database))
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
			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
			}

			conn, err := pgx.Connect(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
			}
			defer conn.Close(ctx)

			testTableName := fmt.Sprintf("test_with_index_%s", tt.name)
			quotedTableName := pgx.Identifier{testTableName}.Sanitize()
			_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			defer func() {
				_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
			}()

			opts := []pgxadapter.Option{pgxadapter.WithTableName(testTableName)}
			for _, idx := range tt.indexes {
				opts = append(opts, pgxadapter.WithIndex(idx...))
			}

			_, err = pgxadapter.NewAdapterWithConn(conn, opts...)
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			for _, expectedIndex := range tt.expectedIndexes {
				var exists bool
				err = conn.QueryRow(ctx,
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

package pgxadapter

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestNewAdapter(t *testing.T) {
	tests := []struct {
		name      string
		tableName string
		wantErr   bool
		errMsg    string
	}{
		{
			name:      "successful_creation_with_default_table",
			tableName: "casbin_test_new_adapter",
			wantErr:   false,
		},
		{
			name:      "successful_creation_with_custom_table",
			tableName: "custom_casbin_table",
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			
			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5432/casbin_test?sslmode=disable"
			}
			
			adapter, err := NewAdapter(dbURL, WithTableName(tt.tableName))
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewAdapter() expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("NewAdapter() unexpected error: %v", err)
			}
			
			t.Cleanup(func() {
				if adapter != nil {
					ctx := context.Background()
					if adapter.pool != nil {
						quotedTableName := pgx.Identifier{tt.tableName}.Sanitize()
						_, _ = adapter.pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
					}
					adapter.Close()
				}
			})
			
			if adapter.tableName != tt.tableName {
				t.Errorf("NewAdapter() tableName = %v, want %v", adapter.tableName, tt.tableName)
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
			tableName: "casbin_test_with_pool",
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
				dbURL = "postgres://postgres:postgres@localhost:5432/casbin_test?sslmode=disable"
			}
			
			pool, err := pgxpool.New(ctx, dbURL)
			if err != nil {
				t.Skipf("Could not connect to test database: %v", err)
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
			
			adapter, err := NewAdapterWithPool(pool, WithTableName(tt.tableName))
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewAdapterWithPool() expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("NewAdapterWithPool() unexpected error: %v", err)
			}
			
			if adapter.tableName != tt.tableName {
				t.Errorf("NewAdapterWithPool() tableName = %v, want %v", adapter.tableName, tt.tableName)
			}
			
			if adapter.pool == nil {
				t.Error("NewAdapterWithPool() expected pool to be set")
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
				dbURL = "postgres://postgres:postgres@localhost:5432/casbin_test?sslmode=disable"
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
			
			adapter, err := NewAdapterWithConn(conn, WithTableName(tt.tableName))
			
			if tt.wantErr {
				if err == nil {
					t.Errorf("NewAdapterWithConn() expected error but got none")
				}
				return
			}
			
			if err != nil {
				t.Fatalf("NewAdapterWithConn() unexpected error: %v", err)
			}
			
			if adapter.tableName != tt.tableName {
				t.Errorf("NewAdapterWithConn() tableName = %v, want %v", adapter.tableName, tt.tableName)
			}
			
			if adapter.conn == nil {
				t.Error("NewAdapterWithConn() expected conn to be set")
			}
		})
	}
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
			
			// Create a minimal adapter struct for testing
			adapter := &PgxAdapter{
				tableName: "default_table",
			}
			
			// Apply the option
			opt := WithTableName(tt.tableName)
			opt(adapter)
			
			if adapter.tableName != tt.expectedTable {
				t.Errorf("WithTableName() set tableName = %v, want %v", adapter.tableName, tt.expectedTable)
			}
		})
	}
}

func TestWithDatabase(t *testing.T) {
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
			
			// Create a minimal adapter struct for testing
			adapter := &PgxAdapter{
				database: "default_db",
			}
			
			// Apply the option
			opt := WithDatabase(tt.database)
			opt(adapter)
			
			if adapter.database != tt.expectedDatabase {
				t.Errorf("WithDatabase() set database = %v, want %v", adapter.database, tt.expectedDatabase)
			}
		})
	}
}

func TestClose(t *testing.T) {
	tests := []struct {
		name     string
		usePool  bool
		useConn  bool
	}{
		{
			name:    "close_with_pool",
			usePool: true,
			useConn: false,
		},
		{
			name:    "close_with_conn",
			usePool: false,
			useConn: true,
		},
		{
			name:    "close_with_neither",
			usePool: false,
			useConn: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			
			ctx := context.Background()
			dbURL := os.Getenv("TEST_DATABASE_URL")
			if dbURL == "" {
				dbURL = "postgres://postgres:postgres@localhost:5432/casbin_test?sslmode=disable"
			}
			
			var adapter *PgxAdapter
			tableName := fmt.Sprintf("casbin_test_close_%s", tt.name)
			
			if tt.usePool {
				pool, err := pgxpool.New(ctx, dbURL)
				if err != nil {
					t.Skipf("Could not connect to test database: %v", err)
				}
				
				adapter, err = NewAdapterWithPool(pool, WithTableName(tableName))
				if err != nil {
					t.Fatalf("Failed to create adapter with pool: %v", err)
				}
				
				t.Cleanup(func() {
					quotedTableName := pgx.Identifier{tableName}.Sanitize()
					_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
				})
				
			} else if tt.useConn {
				conn, err := pgx.Connect(ctx, dbURL)
				if err != nil {
					t.Skipf("Could not connect to test database: %v", err)
				}
				
				adapter, err = NewAdapterWithConn(conn, WithTableName(tableName))
				if err != nil {
					t.Fatalf("Failed to create adapter with conn: %v", err)
				}
				
				t.Cleanup(func() {
					quotedTableName := pgx.Identifier{tableName}.Sanitize()
					_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
				})
				
			} else {
				// Create adapter with neither pool nor conn for testing
				adapter = &PgxAdapter{
					tableName: tableName,
				}
			}
			
			// Call Close() - should not panic
			adapter.Close()
			
			// For pool, verify it's closed
			if tt.usePool && adapter.pool != nil {
				// Try to ping - should fail after close
				err := adapter.pool.Ping(ctx)
				if err == nil {
					t.Error("Close() pool should be closed but ping succeeded")
				}
			}
		})
	}
}
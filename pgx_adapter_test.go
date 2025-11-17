package pgxadapter

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
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
				dbURL = "postgres://postgres:postgres@localhost:5433/casbin_test?sslmode=disable"
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
					if adapter.conn != nil {
						quotedTableName := pgx.Identifier{tt.tableName}.Sanitize()
						_, _ = adapter.conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
					}
					adapter.conn.Close(ctx)
				}
			})

			if adapter.tableName != tt.tableName {
				t.Errorf("NewAdapter() tableName = %v, want %v", adapter.tableName, tt.tableName)
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

			// Create a minimal adapter struct for testing
			adapter := &PgxAdapter{
				database: "default_db",
			}

			// Apply the option
			opt := WithDatabaseName(tt.database)
			opt(adapter)

			if adapter.database != tt.expectedDatabase {
				t.Errorf("WithDatabaseName() set database = %v, want %v", adapter.database, tt.expectedDatabase)
			}
		})
	}
}

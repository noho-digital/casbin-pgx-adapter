package pgxadapter_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	pgxadapter "github.com/noho-digital/casbin-pgx-adapter"
)

// setupBatchAdapterTestDB creates a clean test database connection for batch adapter tests
func setupBatchAdapterTestDB(t *testing.T, tableName string) *pgx.Conn {
	t.Helper()

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
	quotedTableName := pgx.Identifier{tableName}.Sanitize()
	_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")

	t.Cleanup(func() {
		_, _ = conn.Exec(ctx, "DROP TABLE IF EXISTS "+quotedTableName+" CASCADE")
		conn.Close(ctx)
	})

	return conn
}

func TestAddPolicies(t *testing.T) {
	tests := []struct {
		name          string
		sec           string
		ptype         string
		rules         [][]string
		wantErr       bool
		errMsg        string
		expectedCount int
	}{
		{
			name:  "add_multiple_valid_policies",
			sec:   "p",
			ptype: "p",
			rules: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
				{"charlie", "data3", "read"},
			},
			wantErr:       false,
			expectedCount: 3,
		},
		{
			name:          "add_empty_rules",
			sec:           "p",
			ptype:         "p",
			rules:         [][]string{},
			wantErr:       false,
			expectedCount: 0,
		},
		{
			name:  "add_policies_with_empty_fields",
			sec:   "p",
			ptype: "p",
			rules: [][]string{
				{"alice", "", "read"},
				{"bob", "data2", ""},
			},
			wantErr:       false,
			expectedCount: 2,
		},
		{
			name:  "add_grouping_policies",
			sec:   "g",
			ptype: "g",
			rules: [][]string{
				{"alice", "admin"},
				{"bob", "member"},
			},
			wantErr:       false,
			expectedCount: 2,
		},
		{
			name:  "add_duplicate_policies",
			sec:   "p",
			ptype: "p",
			rules: [][]string{
				{"alice", "data1", "read"},
				{"alice", "data1", "read"},
			},
			wantErr:       true,
			errMsg:        "one or more policies already exist",
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_add_batch_%s", tt.name)
			conn := setupBatchAdapterTestDB(t, tableName)

			adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			err = adapter.AddPolicies(tt.sec, tt.ptype, tt.rules)

			if tt.wantErr {
				if err == nil {
					t.Errorf("AddPoliciesCtx() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("AddPoliciesCtx() unexpected error: %v", err)
			}

			// Verify the policies were added
			quotedTableName := pgx.Identifier{tableName}.Sanitize()
			var count int
			query := "SELECT COUNT(*) FROM " + quotedTableName + " WHERE ptype = $1"
			err = conn.QueryRow(ctx, query, tt.ptype).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to verify policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("AddPoliciesCtx() added %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestRemovePolicies(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies [][]string
		sec           string
		ptype         string
		rules         [][]string
		wantErr       bool
		errMsg        string
		expectedCount int
	}{
		{
			name: "remove_multiple_existing_policies",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
				{"p", "charlie", "data3", "read"},
			},
			sec:   "p",
			ptype: "p",
			rules: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			wantErr:       false,
			expectedCount: 1,
		},
		{
			name: "remove_non_existent_policies",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			sec:   "p",
			ptype: "p",
			rules: [][]string{
				{"bob", "data2", "write"},
				{"charlie", "data3", "read"},
			},
			wantErr:       true,
			errMsg:        "no policies found",
			expectedCount: 1,
		},
		{
			name:          "remove_empty_rules",
			setupPolicies: [][]string{},
			sec:           "p",
			ptype:         "p",
			rules:         [][]string{},
			wantErr:       false,
			expectedCount: 0,
		},
		{
			name: "remove_policies_with_null_fields",
			setupPolicies: [][]string{
				{"p", "alice", "data1", ""},
				{"p", "bob", "data2", ""},
			},
			sec:   "p",
			ptype: "p",
			rules: [][]string{
				{"alice", "data1", ""},
			},
			wantErr:       false,
			expectedCount: 1,
		},
		{
			name: "remove_all_policies",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
			},
			sec:   "p",
			ptype: "p",
			rules: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			wantErr:       false,
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_remove_batch_%s", tt.name)
			conn := setupBatchAdapterTestDB(t, tableName)

			adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			// Setup initial policies
			for _, policy := range tt.setupPolicies {
				err = adapter.AddPolicyCtx(ctx, policy[0], policy[0], policy[1:])
				if err != nil {
					t.Fatalf("Failed to setup policy: %v", err)
				}
			}

			err = adapter.RemovePolicies(tt.sec, tt.ptype, tt.rules)

			if tt.wantErr {
				if err == nil {
					t.Errorf("RemovePoliciesCtx() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("RemovePoliciesCtx() unexpected error: %v", err)
			}

			// Verify remaining policies
			quotedTableName := pgx.Identifier{tableName}.Sanitize()
			var count int
			query := "SELECT COUNT(*) FROM " + quotedTableName
			err = conn.QueryRow(ctx, query).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count remaining policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("RemovePoliciesCtx() left %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestAddPoliciesWithPartialDuplicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tableName := "casbin_test_add_batch_partial_duplicates"
	conn := setupBatchAdapterTestDB(t, tableName)

	adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	// Add initial policy
	err = adapter.AddPolicyCtx(ctx, "p", "p", []string{"alice", "data1", "read"})
	if err != nil {
		t.Fatalf("Failed to add initial policy: %v", err)
	}

	// Try to add multiple policies where one is a duplicate
	rules := [][]string{
		{"bob", "data2", "write"},
		{"alice", "data1", "read"}, // This is a duplicate
		{"charlie", "data3", "read"},
	}

	err = adapter.AddPolicies("p", "p", rules)

	if err == nil {
		t.Errorf("AddPoliciesCtx() expected error for partial duplicates but got none")
	}

	// Verify that the transaction was rolled back
	quotedTableName := pgx.Identifier{tableName}.Sanitize()
	var count int
	query := "SELECT COUNT(*) FROM " + quotedTableName
	err = conn.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count policies: %v", err)
	}

	// Should only have the initial policy, not any from the failed batch
	if count != 1 {
		t.Errorf("AddPoliciesCtx() should have rolled back, but found %d policies", count)
	}
}

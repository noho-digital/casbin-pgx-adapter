package pgxadapter_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/casbin/casbin/v2/model"
	"github.com/jackc/pgx/v5"
	pgxadapter "github.com/noho-digital/casbin-pgx-adapter"
)

// setupFilteredAdapterTestDB creates a clean test database connection for filtered adapter tests
func setupFilteredAdapterTestDB(t *testing.T, tableName string) *pgx.Conn {
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

func TestLoadFilteredPolicy(t *testing.T) {
	tests := []struct {
		name             string
		setupPolicies    [][]string
		filter           pgxadapter.Filter
		expectedPolicies [][]string
		wantErr          bool
	}{
		{
			name: "filter_by_subject",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data2", "write"},
				{"p", "bob", "data1", "read"},
			},
			filter: pgxadapter.Filter{
				V0: []string{"alice"},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
				{"alice", "data2", "write"},
			},
			wantErr: false,
		},
		{
			name: "filter_by_subject_and_object",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data2", "write"},
				{"p", "bob", "data1", "read"},
			},
			filter: pgxadapter.Filter{
				V0: []string{"alice"},
				V1: []string{"data1"},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
			},
			wantErr: false,
		},
		{
			name: "filter_with_or_on_subject",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
				{"p", "charlie", "data3", "read"},
			},
			filter: pgxadapter.Filter{
				V0: []string{"alice", "bob"},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			wantErr: false,
		},
		{
			name: "filter_with_or_on_object",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data2", "write"},
				{"p", "bob", "data1", "read"},
			},
			filter: pgxadapter.Filter{
				V1: []string{"data1", "data2"},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
				{"alice", "data2", "write"},
				{"bob", "data1", "read"},
			},
			wantErr: false,
		},
		{
			name: "filter_grouping_policies",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"g", "alice", "admin"},
				{"g", "bob", "member"},
			},
			filter: pgxadapter.Filter{
				Ptype: []string{"g"},
				V0:    []string{"alice"},
			},
			expectedPolicies: [][]string{
				{"alice", "admin"},
			},
			wantErr: false,
		},
		{
			name: "filter_no_match",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			filter: pgxadapter.Filter{
				V0: []string{"bob"},
			},
			expectedPolicies: [][]string{},
			wantErr:          false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_filtered_%s", tt.name)
			conn := setupFilteredAdapterTestDB(t, tableName)

			adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			// Setup initial policies
			for _, policy := range tt.setupPolicies {
				err = adapter.AddPolicy(policy[0], policy[0], policy[1:])
				if err != nil {
					t.Fatalf("Failed to setup policy: %v", err)
				}
			}

			// Create model and load filtered policies
			m, _ := model.NewModelFromString(pgxadapter.TestModelText)

			err = adapter.LoadFilteredPolicy(m, tt.filter)

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadFilteredPolicy() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("LoadFilteredPolicy() unexpected error: %v", err)
				return
			}

			// Collect all loaded policies
			var loadedPolicies [][]string
			for _, ast := range m["p"] {
				loadedPolicies = append(loadedPolicies, ast.Policy...)
			}
			for _, ast := range m["g"] {
				loadedPolicies = append(loadedPolicies, ast.Policy...)
			}

			// Compare policy content
			if len(loadedPolicies) != len(tt.expectedPolicies) {
				t.Errorf("LoadFilteredPolicy() loaded %d policies, want %d. Got: %v, Expected: %v",
					len(loadedPolicies), len(tt.expectedPolicies), loadedPolicies, tt.expectedPolicies)
				return
			}

			// Check if all expected policies are present
			for _, expectedPolicy := range tt.expectedPolicies {
				found := false
				for _, loadedPolicy := range loadedPolicies {
					if slices.Equal(loadedPolicy, expectedPolicy) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected policy %v not found in loaded policies %v", expectedPolicy, loadedPolicies)
				}
			}
		})
	}
}

func TestLoadFilteredPolicyInvalidFilter(t *testing.T) {
	t.Parallel()

	tableName := "casbin_test_filtered_invalid"
	conn := setupFilteredAdapterTestDB(t, tableName)

	adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	m, _ := model.NewModelFromString(pgxadapter.TestModelText)

	// Try with invalid filter type
	err = adapter.LoadFilteredPolicy(m, "invalid filter")

	if err == nil {
		t.Errorf("LoadFilteredPolicy() expected error for invalid filter type but got none")
	}
}

func TestIsFiltered(t *testing.T) {
	tests := []struct {
		name             string
		setupPolicies    [][]string
		filter           pgxadapter.Filter
		expectedFiltered bool
	}{
		{
			name: "filtered_with_filter",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			filter: pgxadapter.Filter{
				V0: []string{"alice"},
			},
			expectedFiltered: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_is_filtered_%s", tt.name)
			conn := setupFilteredAdapterTestDB(t, tableName)

			adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			// Setup initial policies
			for _, policy := range tt.setupPolicies {
				err = adapter.AddPolicy(policy[0], policy[0], policy[1:])
				if err != nil {
					t.Fatalf("Failed to setup policy: %v", err)
				}
			}

			// Create model and load filtered policies
			m, _ := model.NewModelFromString(pgxadapter.TestModelText)

			err = adapter.LoadFilteredPolicy(m, tt.filter)
			if err != nil {
				t.Fatalf("LoadFilteredPolicy() unexpected error: %v", err)
			}

			if adapter.IsFiltered() != tt.expectedFiltered {
				t.Errorf("IsFiltered() = %v, want %v", adapter.IsFiltered(), tt.expectedFiltered)
			}
		})
	}
}

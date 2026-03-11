package pgxadapter_test

import (
	"context"
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"
)

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
			adapter, db := setupTestAdapter(t, tableName)

			err := adapter.AddPolicies(tt.sec, tt.ptype, tt.rules)

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
			var count int
			q, args, _ := testPsql.Select("COUNT(*)").From(tableName).Where(sq.Eq{"ptype": tt.ptype}).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&count)
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
			adapter, db := setupTestAdapter(t, tableName)

			// Setup initial policies
			for _, policy := range tt.setupPolicies {
				err := adapter.AddPolicyCtx(ctx, policy[0], policy[0], policy[1:])
				if err != nil {
					t.Fatalf("Failed to setup policy: %v", err)
				}
			}

			err := adapter.RemovePolicies(tt.sec, tt.ptype, tt.rules)

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
			var count int
			q, args, _ := testPsql.Select("COUNT(*)").From(tableName).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count remaining policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("RemovePoliciesCtx() left %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestRemovePoliciesWithEmptyStringsInDB(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tableName := "casbin_test_remove_batch_empty_str_db"
	adapter, db := setupTestAdapter(t, tableName)

	// Insert rows directly with empty strings instead of NULLs for unused fields
	q, args, _ := testPsql.Insert(tableName).
		Columns("ptype", "v0", "v1", "v2", "v3", "v4", "v5").
		Values("p", "alice", "data1", "read", "", "", "").
		ToSql()
	_, err := db.ExecContext(ctx, q, args...)
	if err != nil {
		t.Fatalf("Failed to insert test row: %v", err)
	}

	q, args, _ = testPsql.Insert(tableName).
		Columns("ptype", "v0", "v1", "v2", "v3", "v4", "v5").
		Values("p", "bob", "data2", "write", "", "", "").
		ToSql()
	_, err = db.ExecContext(ctx, q, args...)
	if err != nil {
		t.Fatalf("Failed to insert test row: %v", err)
	}

	// Try to remove policies - should work even though DB has '' instead of NULL
	err = adapter.RemovePolicies("p", "p", [][]string{
		{"alice", "data1", "read"},
	})
	if err != nil {
		t.Errorf("RemovePolicies() failed to delete row with empty strings in DB: %v", err)
	}

	// Verify only bob's row remains
	var count int
	q, args, _ = testPsql.Select("COUNT(*)").From(tableName).ToSql()
	err = db.QueryRowContext(ctx, q, args...).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}
	if count != 1 {
		t.Errorf("RemovePolicies() left %d rows, want 1", count)
	}
}

func TestAddPoliciesWithPartialDuplicates(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	tableName := "casbin_test_add_batch_partial_duplicates"
	adapter, db := setupTestAdapter(t, tableName)

	// Add initial policy
	err := adapter.AddPolicyCtx(ctx, "p", "p", []string{"alice", "data1", "read"})
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
	var count int
	q, args, _ := testPsql.Select("COUNT(*)").From(tableName).ToSql()
	err = db.QueryRowContext(ctx, q, args...).Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count policies: %v", err)
	}

	// Should only have the initial policy, not any from the failed batch
	if count != 1 {
		t.Errorf("AddPoliciesCtx() should have rolled back, but found %d policies", count)
	}
}

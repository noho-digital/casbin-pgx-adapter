package pgxadapter_test

import (
	"context"
	"fmt"
	"testing"

	sq "github.com/Masterminds/squirrel"
	"github.com/casbin/casbin/v3"
	"github.com/casbin/casbin/v3/model"
)

func TestLoadPolicy(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies [][]string
		expectedCount int
		wantErr       bool
	}{
		{
			name:          "load_empty_policies",
			setupPolicies: [][]string{},
			expectedCount: 0,
			wantErr:       false,
		},
		{
			name: "load_single_policy",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			expectedCount: 1,
			wantErr:       false,
		},
		{
			name: "load_multiple_policies",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
				{"g", "alice", "admin"},
			},
			expectedCount: 3,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_load_%s", tt.name)
			adapter, db := setupTestAdapter(t, tableName)

			// Insert test data using the adapter
			for _, policy := range tt.setupPolicies {
				if len(policy) > 0 {
					sec := policy[0]
					ptype := policy[0]
					rule := policy[1:]
					err := adapter.AddPolicy(sec, ptype, rule)
					if err != nil {
						t.Fatalf("Failed to insert test policy: %v", err)
					}
				}
			}

			// Create model and load policies
			m, _ := model.NewModelFromString(TestModelText)

			err := adapter.LoadPolicy(m)

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadPolicy() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("LoadPolicy() unexpected error: %v", err)
			}

			// Verify loaded policies count
			ctx := context.Background()
			var count int
			q, args, _ := testPsql.Select("COUNT(*)").From(tableName).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("LoadPolicy() loaded %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestSavePolicy(t *testing.T) {
	tests := []struct {
		name           string
		policies       [][]string
		groupPolicies  [][]string
		expectedPCount int
		expectedGCount int
		wantErr        bool
	}{
		{
			name:           "save_empty_model",
			policies:       [][]string{},
			groupPolicies:  [][]string{},
			expectedPCount: 0,
			expectedGCount: 0,
			wantErr:        false,
		},
		{
			name: "save_policies_only",
			policies: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			groupPolicies:  [][]string{},
			expectedPCount: 2,
			expectedGCount: 0,
			wantErr:        false,
		},
		{
			name:     "save_group_policies_only",
			policies: [][]string{},
			groupPolicies: [][]string{
				{"alice", "admin"},
				{"bob", "member"},
			},
			expectedPCount: 0,
			expectedGCount: 2,
			wantErr:        false,
		},
		{
			name: "save_mixed_policies",
			policies: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			groupPolicies: [][]string{
				{"alice", "admin"},
				{"bob", "member"},
			},
			expectedPCount: 2,
			expectedGCount: 2,
			wantErr:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_save_%s", tt.name)
			adapter, db := setupTestAdapter(t, tableName)

			// Create model and enforcer
			m, _ := model.NewModelFromString(TestModelText)
			e, err := casbin.NewEnforcer(m, adapter)
			if err != nil {
				t.Fatalf("Failed to create enforcer: %v", err)
			}

			// Add policies to enforcer
			for _, policy := range tt.policies {
				_, err := e.AddPolicy(policy)
				if err != nil {
					t.Fatalf("Failed to add policy: %v", err)
				}
			}

			for _, groupPolicy := range tt.groupPolicies {
				_, err := e.AddGroupingPolicy(groupPolicy)
				if err != nil {
					t.Fatalf("Failed to add grouping policy: %v", err)
				}
			}

			// Save policies
			err = adapter.SavePolicy(e.GetModel())

			if tt.wantErr {
				if err == nil {
					t.Errorf("SavePolicy() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("SavePolicy() unexpected error: %v", err)
			}

			// Verify saved policies
			ctx := context.Background()
			var pCount int
			q, args, _ := testPsql.Select("COUNT(*)").From(tableName).Where(sq.Eq{"ptype": "p"}).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&pCount)
			if err != nil {
				t.Fatalf("Failed to count p policies: %v", err)
			}

			if pCount != tt.expectedPCount {
				t.Errorf("SavePolicy() saved %d p policies, want %d", pCount, tt.expectedPCount)
			}

			var gCount int
			q, args, _ = testPsql.Select("COUNT(*)").From(tableName).Where(sq.Eq{"ptype": "g"}).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&gCount)
			if err != nil {
				t.Fatalf("Failed to count g policies: %v", err)
			}

			if gCount != tt.expectedGCount {
				t.Errorf("SavePolicy() saved %d g policies, want %d", gCount, tt.expectedGCount)
			}
		})
	}
}

func TestAddPolicy(t *testing.T) {
	tests := []struct {
		name    string
		sec     string
		ptype   string
		rule    []string
		wantErr bool
		errMsg  string
	}{
		{
			name:    "add_valid_policy",
			sec:     "p",
			ptype:   "p",
			rule:    []string{"alice", "data1", "read"},
			wantErr: false,
		},
		{
			name:    "add_policy_with_empty_fields",
			sec:     "p",
			ptype:   "p",
			rule:    []string{"alice", "", "read"},
			wantErr: false,
		},
		{
			name:    "add_grouping_policy",
			sec:     "g",
			ptype:   "g",
			rule:    []string{"alice", "admin"},
			wantErr: false,
		},
		{
			name:    "add_duplicate_policy",
			sec:     "p",
			ptype:   "p",
			rule:    []string{"alice", "data1", "read"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_add_%s", tt.name)
			adapter, db := setupTestAdapter(t, tableName)

			// Add the policy first time if testing duplicate
			if tt.name == "add_duplicate_policy" {
				err := adapter.AddPolicy(tt.sec, tt.ptype, tt.rule)
				if err != nil {
					t.Fatalf("Failed to add initial policy for duplicate test: %v", err)
				}
			}

			err := adapter.AddPolicy(tt.sec, tt.ptype, tt.rule)

			if tt.wantErr {
				if err == nil {
					t.Errorf("AddPolicy() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("AddPolicy() unexpected error: %v", err)
			}

			// Verify the policy was added
			ctx := context.Background()
			var count int
			q, args, _ := testPsql.Select("COUNT(*)").From(tableName).Where(sq.Eq{"ptype": tt.ptype}).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to verify policy: %v", err)
			}

			if count != 1 {
				t.Errorf("AddPolicy() added %d policies, want 1", count)
			}
		})
	}
}

func TestRemovePolicy(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies [][]string
		sec           string
		ptype         string
		rule          []string
		wantErr       bool
		errMsg        string
		expectedCount int
	}{
		{
			name: "remove_existing_policy",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
			},
			sec:           "p",
			ptype:         "p",
			rule:          []string{"alice", "data1", "read"},
			wantErr:       false,
			expectedCount: 1,
		},
		{
			name:          "remove_non_existent_policy",
			setupPolicies: [][]string{},
			sec:           "p",
			ptype:         "p",
			rule:          []string{"alice", "data1", "read"},
			wantErr:       false,
			expectedCount: 0,
		},
		{
			name: "remove_policy_with_null_fields",
			setupPolicies: [][]string{
				{"p", "alice", "data1", ""},
			},
			sec:           "p",
			ptype:         "p",
			rule:          []string{"alice", "data1", ""},
			wantErr:       false,
			expectedCount: 0,
		},
	}

	// Run table-driven tests
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_remove_%s", tt.name)
			adapter, db := setupTestAdapter(t, tableName)

			// Setup initial policies
			for _, policy := range tt.setupPolicies {
				err := adapter.AddPolicy(policy[0], policy[0], policy[1:])
				if err != nil {
					t.Fatalf("Failed to setup policy: %v", err)
				}
			}

			err := adapter.RemovePolicy(tt.sec, tt.ptype, tt.rule)

			if tt.wantErr {
				if err == nil {
					t.Errorf("RemovePolicy() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("RemovePolicy() unexpected error: %v", err)
			}

			// Verify remaining policies
			ctx := context.Background()
			var count int
			q, args, _ := testPsql.Select("COUNT(*)").From(tableName).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count remaining policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("RemovePolicy() left %d policies, want %d", count, tt.expectedCount)
			}
		})
	}

	// Test: remove policy when DB has empty strings instead of NULLs
	t.Run("remove_policy_with_empty_strings_in_db", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		tableName := "casbin_test_remove_empty_str_db"
		adapter, db := setupTestAdapter(t, tableName)

		// Insert a row directly with empty strings instead of NULLs for unused fields
		q, args, _ := testPsql.Insert(tableName).
			Columns("ptype", "v0", "v1", "v2", "v3", "v4", "v5").
			Values("p", "alice", "data1", "read", "", "", "").
			ToSql()
		_, err := db.ExecContext(ctx, q, args...)
		if err != nil {
			t.Fatalf("Failed to insert test row with empty strings: %v", err)
		}

		// Verify the row was inserted
		var count int
		q, args, _ = testPsql.Select("COUNT(*)").From(tableName).ToSql()
		err = db.QueryRowContext(ctx, q, args...).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count rows: %v", err)
		}
		if count != 1 {
			t.Fatalf("Expected 1 row after insert, got %d", count)
		}

		// Try to remove the policy - this should work even though DB has '' instead of NULL
		err = adapter.RemovePolicy("p", "p", []string{"alice", "data1", "read"})
		if err != nil {
			t.Errorf("RemovePolicy() failed to delete row with empty strings in DB: %v", err)
		}

		// Verify the row was deleted
		q, args, _ = testPsql.Select("COUNT(*)").From(tableName).ToSql()
		err = db.QueryRowContext(ctx, q, args...).Scan(&count)
		if err != nil {
			t.Fatalf("Failed to count rows after delete: %v", err)
		}
		if count != 0 {
			t.Errorf("RemovePolicy() left %d rows, want 0", count)
		}
	})
}

func TestRemoveFilteredPolicy(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies [][]string
		sec           string
		ptype         string
		fieldIndex    int
		fieldValues   []string
		wantErr       bool
		errMsg        string
		expectedCount int
	}{
		{
			name: "remove_by_subject",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data1", "write"},
				{"p", "bob", "data2", "read"},
			},
			sec:           "p",
			ptype:         "p",
			fieldIndex:    0,
			fieldValues:   []string{"alice"},
			wantErr:       false,
			expectedCount: 1, // only bob's policy remains
		},
		{
			name: "remove_by_subject_and_object",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data1", "write"},
				{"p", "alice", "data2", "read"},
				{"p", "bob", "data1", "read"},
			},
			sec:           "p",
			ptype:         "p",
			fieldIndex:    0,
			fieldValues:   []string{"alice", "data1"},
			wantErr:       false,
			expectedCount: 2, // alice+data2 and bob+data1 remain
		},
		{
			// Removal of policies should be an idempotent operation.
			// Watchers must be able to run RemovePolicy to affect the
			// in-memory policy without worrying about the state of the database.
			name: "remove_with_no_matches",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			sec:           "p",
			ptype:         "p",
			fieldIndex:    0,
			fieldValues:   []string{"bob"},
			wantErr:       false,
			expectedCount: 1,
		},
		{
			name:          "remove_with_invalid_field_index",
			setupPolicies: [][]string{},
			sec:           "p",
			ptype:         "p",
			fieldIndex:    7,
			fieldValues:   []string{"alice"},
			wantErr:       true,
			errMsg:        "invalid field index",
			expectedCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_filter_%s", tt.name)
			adapter, db := setupTestAdapter(t, tableName)

			// Setup initial policies
			for _, policy := range tt.setupPolicies {
				err := adapter.AddPolicy(policy[0], policy[0], policy[1:])
				if err != nil {
					t.Fatalf("Failed to setup policy: %v", err)
				}
			}

			err := adapter.RemoveFilteredPolicy(tt.sec, tt.ptype, tt.fieldIndex, tt.fieldValues...)

			if tt.wantErr {
				if err == nil {
					t.Errorf("RemoveFilteredPolicy() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("RemoveFilteredPolicy() unexpected error: %v", err)
			}

			// Verify remaining policies
			ctx := context.Background()
			var count int
			q, args, _ := testPsql.Select("COUNT(*)").From(tableName).ToSql()
			err = db.QueryRowContext(ctx, q, args...).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count remaining policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("RemoveFilteredPolicy() left %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

package pgxadapter

import (
	"context"
	"fmt"
	"testing"

	"github.com/casbin/casbin/v2"
	"github.com/casbin/casbin/v2/model"
	"github.com/jackc/pgx/v5"
)

func TestLoadPolicyCtx(t *testing.T) {
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

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_load_%s", tt.name)
			pool := setupAdapterTestDB(t, tableName)

			adapter, err := NewAdapterWithPool(pool, WithTableName(tableName))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			// Insert test data directly into database
			for _, policy := range tt.setupPolicies {
				// Use the adapter's AddPolicyCtx method to insert test data properly
				if len(policy) > 0 {
					sec := policy[0]
					ptype := policy[0]
					rule := policy[1:]
					err := adapter.AddPolicyCtx(ctx, sec, ptype, rule)
					if err != nil {
						t.Fatalf("Failed to insert test policy: %v", err)
					}
				}
			}

			// Create model and load policies
			modelText := `
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
			m, _ := model.NewModelFromString(modelText)

			err = adapter.LoadPolicyCtx(ctx, m)

			if tt.wantErr {
				if err == nil {
					t.Errorf("LoadPolicyCtx() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("LoadPolicyCtx() unexpected error: %v", err)
			}

			// Verify loaded policies count
			var count int
			quotedTableName := pgx.Identifier{tableName}.Sanitize()
			err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM "+quotedTableName).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("LoadPolicyCtx() loaded %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestSavePolicyCtx(t *testing.T) {
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

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_save_%s", tt.name)
			pool := setupAdapterTestDB(t, tableName)

			adapter, err := NewAdapterWithPool(pool, WithTableName(tableName))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			// Create model and enforcer
			modelText := `
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
			m, _ := model.NewModelFromString(modelText)
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
			err = adapter.SavePolicyCtx(ctx, e.GetModel())

			if tt.wantErr {
				if err == nil {
					t.Errorf("SavePolicyCtx() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("SavePolicyCtx() unexpected error: %v", err)
			}

			// Verify saved policies
			quotedTableName := pgx.Identifier{tableName}.Sanitize()
			var pCount int
			err = pool.QueryRow(ctx,
				"SELECT COUNT(*) FROM "+quotedTableName+" WHERE ptype = 'p'",
			).Scan(&pCount)
			if err != nil {
				t.Fatalf("Failed to count p policies: %v", err)
			}

			if pCount != tt.expectedPCount {
				t.Errorf("SavePolicyCtx() saved %d p policies, want %d", pCount, tt.expectedPCount)
			}

			var gCount int
			err = pool.QueryRow(ctx,
				"SELECT COUNT(*) FROM "+quotedTableName+" WHERE ptype = 'g'",
			).Scan(&gCount)
			if err != nil {
				t.Fatalf("Failed to count g policies: %v", err)
			}

			if gCount != tt.expectedGCount {
				t.Errorf("SavePolicyCtx() saved %d g policies, want %d", gCount, tt.expectedGCount)
			}
		})
	}
}

func TestAddPolicyCtx(t *testing.T) {
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
			wantErr: true,
			errMsg:  "policy already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_add_%s", tt.name)
			pool := setupAdapterTestDB(t, tableName)

			adapter, err := NewAdapterWithPool(pool, WithTableName(tableName))
			if err != nil {
				t.Fatalf("Failed to create adapter: %v", err)
			}

			// Add the policy first time if testing duplicate
			if tt.name == "add_duplicate_policy" {
				err = adapter.AddPolicyCtx(ctx, tt.sec, tt.ptype, tt.rule)
				if err != nil {
					t.Fatalf("Failed to add initial policy for duplicate test: %v", err)
				}
			}

			err = adapter.AddPolicyCtx(ctx, tt.sec, tt.ptype, tt.rule)

			if tt.wantErr {
				if err == nil {
					t.Errorf("AddPolicyCtx() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("AddPolicyCtx() unexpected error: %v", err)
			}

			// Verify the policy was added
			quotedTableName := pgx.Identifier{tableName}.Sanitize()
			var count int
			query := "SELECT COUNT(*) FROM " + quotedTableName + " WHERE ptype = $1"
			err = pool.QueryRow(ctx, query, tt.ptype).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to verify policy: %v", err)
			}

			if count != 1 {
				t.Errorf("AddPolicyCtx() added %d policies, want 1", count)
			}
		})
	}
}

func TestRemovePolicyCtx(t *testing.T) {
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
			wantErr:       true,
			errMsg:        "policy not found",
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

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_remove_%s", tt.name)
			pool := setupAdapterTestDB(t, tableName)

			adapter, err := NewAdapterWithPool(pool, WithTableName(tableName))
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

			err = adapter.RemovePolicyCtx(ctx, tt.sec, tt.ptype, tt.rule)

			if tt.wantErr {
				if err == nil {
					t.Errorf("RemovePolicyCtx() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("RemovePolicyCtx() unexpected error: %v", err)
			}

			// Verify remaining policies
			quotedTableName := pgx.Identifier{tableName}.Sanitize()
			var count int
			query := "SELECT COUNT(*) FROM " + quotedTableName
			err = pool.QueryRow(ctx, query).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count remaining policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("RemovePolicyCtx() left %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

func TestRemoveFilteredPolicyCtx(t *testing.T) {
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
			name: "remove_with_no_matches",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			sec:           "p",
			ptype:         "p",
			fieldIndex:    0,
			fieldValues:   []string{"bob"},
			wantErr:       true,
			errMsg:        "no matching policies found",
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

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_filter_%s", tt.name)
			pool := setupAdapterTestDB(t, tableName)

			adapter, err := NewAdapterWithPool(pool, WithTableName(tableName))
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

			err = adapter.RemoveFilteredPolicyCtx(ctx, tt.sec, tt.ptype, tt.fieldIndex, tt.fieldValues...)

			if tt.wantErr {
				if err == nil {
					t.Errorf("RemoveFilteredPolicyCtx() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("RemoveFilteredPolicyCtx() unexpected error: %v", err)
			}

			// Verify remaining policies
			quotedTableName := pgx.Identifier{tableName}.Sanitize()
			var count int
			query := "SELECT COUNT(*) FROM " + quotedTableName
			err = pool.QueryRow(ctx, query).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count remaining policies: %v", err)
			}

			if count != tt.expectedCount {
				t.Errorf("RemoveFilteredPolicyCtx() left %d policies, want %d", count, tt.expectedCount)
			}
		})
	}
}

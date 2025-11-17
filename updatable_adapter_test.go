package pgxadapter_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	pgxadapter "github.com/noho-digital/casbin-pgx-adapter"
)

func TestUpdatePolicy(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies [][]string
		oldRule       []string
		newRule       []string
		wantErr       bool
		errMsg        string
	}{
		{
			name: "update_existing_policy",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			oldRule: []string{"alice", "data1", "read"},
			newRule: []string{"alice", "data1", "write"},
			wantErr: false,
		},
		{
			name: "update_non_existent_policy",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			oldRule: []string{"bob", "data2", "write"},
			newRule: []string{"bob", "data2", "read"},
			wantErr: true,
			errMsg:  "policy not found",
		},
		{
			name: "update_policy_with_null_fields",
			setupPolicies: [][]string{
				{"p", "alice", "data1", ""},
			},
			oldRule: []string{"alice", "data1", ""},
			newRule: []string{"alice", "data1", "read"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_update_%s", tt.name)
			conn := setupTestDB(t, tableName)

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

			err = adapter.UpdatePolicy("p", "p", tt.oldRule, tt.newRule)

			if tt.wantErr {
				if err == nil {
					t.Errorf("UpdatePolicy() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("UpdatePolicy() unexpected error: %v", err)
			}

			// Verify the policy was updated
			ctx := context.Background()
			var count int
			quotedTableName := adapter.GetTableName()
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE ptype = $1 AND v0 = $2 AND v1 = $3", quotedTableName)

			if len(tt.newRule) >= 3 {
				query = fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE ptype = $1 AND v0 = $2 AND v1 = $3 AND v2 = $4", quotedTableName)
				err = conn.QueryRow(ctx, query, "p", tt.newRule[0], tt.newRule[1], tt.newRule[2]).Scan(&count)
			} else if len(tt.newRule) >= 2 {
				err = conn.QueryRow(ctx, query, "p", tt.newRule[0], tt.newRule[1]).Scan(&count)
			}

			if err != nil {
				t.Fatalf("Failed to verify policy: %v", err)
			}

			if count != 1 {
				t.Errorf("UpdatePolicy() found %d policies with new values, want 1", count)
			}
		})
	}
}

func TestUpdatePolicies(t *testing.T) {
	tests := []struct {
		name          string
		setupPolicies [][]string
		oldRules      [][]string
		newRules      [][]string
		wantErr       bool
		errMsg        string
	}{
		{
			name: "update_multiple_existing_policies",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
			},
			oldRules: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			newRules: [][]string{
				{"alice", "data1", "write"},
				{"bob", "data2", "read"},
			},
			wantErr: false,
		},
		{
			name: "update_with_mismatched_lengths",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			oldRules: [][]string{
				{"alice", "data1", "read"},
			},
			newRules: [][]string{
				{"alice", "data1", "write"},
				{"bob", "data2", "read"},
			},
			wantErr: true,
			errMsg:  "same length",
		},
		{
			name:          "update_empty_rules",
			setupPolicies: [][]string{},
			oldRules:      [][]string{},
			newRules:      [][]string{},
			wantErr:       false,
		},
		{
			name: "update_with_one_non_existent",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			oldRules: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			newRules: [][]string{
				{"alice", "data1", "write"},
				{"bob", "data2", "read"},
			},
			wantErr: true,
			errMsg:  "policy not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_updates_%s", tt.name)
			conn := setupTestDB(t, tableName)

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

			// Count before update for rollback verification
			var countBefore int
			quotedTableName := adapter.GetTableName()
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s", quotedTableName)
			err = conn.QueryRow(ctx, query).Scan(&countBefore)
			if err != nil {
				t.Fatalf("Failed to count policies before: %v", err)
			}

			err = adapter.UpdatePolicies("p", "p", tt.oldRules, tt.newRules)

			if tt.wantErr {
				if err == nil {
					t.Errorf("UpdatePolicies() expected error but got none")
				}

				// Verify transaction rollback - count should be unchanged
				var countAfter int
				err = conn.QueryRow(ctx, query).Scan(&countAfter)
				if err != nil {
					t.Fatalf("Failed to count policies after: %v", err)
				}

				if countAfter != countBefore {
					t.Errorf("UpdatePolicies() should have rolled back, but count changed from %d to %d", countBefore, countAfter)
				}
				return
			}

			if err != nil {
				t.Errorf("UpdatePolicies() unexpected error: %v", err)
			}
		})
	}
}

func TestUpdateFilteredPolicies(t *testing.T) {
	tests := []struct {
		name            string
		setupPolicies   [][]string
		newRules        [][]string
		fieldIndex      int
		fieldValues     []string
		wantErr         bool
		expectedDeleted [][]string
	}{
		{
			name: "update_policies_by_subject",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data2", "write"},
				{"p", "bob", "data1", "read"},
			},
			newRules: [][]string{
				{"alice", "data3", "read"},
			},
			fieldIndex: 0,
			fieldValues: []string{"alice"},
			wantErr:    false,
			expectedDeleted: [][]string{
				{"alice", "data1", "read"},
				{"alice", "data2", "write"},
			},
		},
		{
			name: "update_policies_by_subject_and_object",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data1", "write"},
				{"p", "alice", "data2", "read"},
			},
			newRules: [][]string{
				{"alice", "data1", "admin"},
			},
			fieldIndex: 0,
			fieldValues: []string{"alice", "data1"},
			wantErr:    false,
			expectedDeleted: [][]string{
				{"alice", "data1", "read"},
				{"alice", "data1", "write"},
			},
		},
		{
			name: "update_with_invalid_field_index",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			newRules: [][]string{
				{"bob", "data2", "write"},
			},
			fieldIndex:      7,
			fieldValues:     []string{"alice"},
			wantErr:         true,
			expectedDeleted: nil,
		},
		{
			name: "update_with_no_matches",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
			},
			newRules: [][]string{
				{"charlie", "data3", "write"},
			},
			fieldIndex:      0,
			fieldValues:     []string{"bob"},
			wantErr:         false,
			expectedDeleted: [][]string{},
		},
		{
			name: "update_with_empty_new_rules",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data2", "write"},
			},
			newRules:    [][]string{},
			fieldIndex:  0,
			fieldValues: []string{"alice"},
			wantErr:     false,
			expectedDeleted: [][]string{
				{"alice", "data1", "read"},
				{"alice", "data2", "write"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			tableName := fmt.Sprintf("casbin_test_update_filtered_%s", tt.name)
			conn := setupTestDB(t, tableName)

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

			deletedPolicies, err := adapter.UpdateFilteredPolicies("p", "p", tt.newRules, tt.fieldIndex, tt.fieldValues...)

			if tt.wantErr {
				if err == nil {
					t.Errorf("UpdateFilteredPolicies() expected error but got none")
				}
				return
			}

			if err != nil {
				t.Errorf("UpdateFilteredPolicies() unexpected error: %v", err)
				return
			}

			// Check deleted policies
			if len(deletedPolicies) != len(tt.expectedDeleted) {
				t.Errorf("UpdateFilteredPolicies() returned %d deleted policies, want %d. Got: %v, Expected: %v",
					len(deletedPolicies), len(tt.expectedDeleted), deletedPolicies, tt.expectedDeleted)
				return
			}

			for _, expectedPolicy := range tt.expectedDeleted {
				found := false
				for _, deletedPolicy := range deletedPolicies {
					if slices.Equal(deletedPolicy, expectedPolicy) {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected deleted policy %v not found in %v", expectedPolicy, deletedPolicies)
				}
			}

			// Verify new policies were inserted
			quotedTableName := adapter.GetTableName()
			var count int
			query := fmt.Sprintf("SELECT COUNT(*) FROM %s", quotedTableName)
			err = conn.QueryRow(ctx, query).Scan(&count)
			if err != nil {
				t.Fatalf("Failed to count policies: %v", err)
			}

			expectedCount := len(tt.setupPolicies) - len(tt.expectedDeleted) + len(tt.newRules)
			if count != expectedCount {
				t.Errorf("UpdateFilteredPolicies() resulted in %d policies, want %d", count, expectedCount)
			}
		})
	}
}

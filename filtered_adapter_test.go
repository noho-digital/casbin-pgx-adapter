package pgxadapter_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/casbin/casbin/v2/model"
	pgxadapter "github.com/noho-digital/casbin-pgx-adapter"
)

func TestLoadFilteredPolicy(t *testing.T) {
	tests := []struct {
		name             string
		setupPolicies    [][]string
		filter           any
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
		{
			name: "batch_filter_or_subjects",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
				{"p", "charlie", "data3", "delete"},
			},
			filter: pgxadapter.BatchFilter{
				Filters: []pgxadapter.Filter{
					{V0: []string{"alice"}},
					{V0: []string{"bob"}},
				},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "write"},
			},
			wantErr: false,
		},
		{
			name: "batch_filter_different_conditions",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "alice", "data2", "write"},
				{"p", "bob", "data1", "read"},
				{"p", "bob", "data2", "delete"},
			},
			filter: pgxadapter.BatchFilter{
				Filters: []pgxadapter.Filter{
					{V0: []string{"alice"}, V1: []string{"data1"}},
					{V0: []string{"bob"}, V2: []string{"delete"}},
				},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
				{"bob", "data2", "delete"},
			},
			wantErr: false,
		},
		{
			name: "batch_filter_with_ptype",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"g", "alice", "admin"},
				{"g", "bob", "member"},
			},
			filter: pgxadapter.BatchFilter{
				Filters: []pgxadapter.Filter{
					{Ptype: []string{"p"}},
					{Ptype: []string{"g"}, V0: []string{"alice"}},
				},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
				{"alice", "admin"},
			},
			wantErr: false,
		},
		{
			name: "batch_filter_pointer",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
			},
			filter: &pgxadapter.BatchFilter{
				Filters: []pgxadapter.Filter{
					{V0: []string{"alice"}},
				},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
			},
			wantErr: false,
		},
		{
			name: "filter_slice",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
				{"p", "charlie", "data3", "delete"},
			},
			filter: []pgxadapter.Filter{
				{V0: []string{"alice"}},
				{V0: []string{"charlie"}},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
				{"charlie", "data3", "delete"},
			},
			wantErr: false,
		},
		{
			name: "filter_pointer",
			setupPolicies: [][]string{
				{"p", "alice", "data1", "read"},
				{"p", "bob", "data2", "write"},
			},
			filter: &pgxadapter.Filter{
				V0: []string{"alice"},
			},
			expectedPolicies: [][]string{
				{"alice", "data1", "read"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tableName := fmt.Sprintf("casbin_test_filtered_%s", tt.name)
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

			// Create model and load filtered policies
			m, _ := model.NewModelFromString(TestModelText)

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
	conn := setupTestDB(t, tableName)

	adapter, err := pgxadapter.NewAdapterWithConn(conn, pgxadapter.WithTableName(tableName))
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}

	m, _ := model.NewModelFromString(TestModelText)

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

			// Create model and load filtered policies
			m, _ := model.NewModelFromString(TestModelText)

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

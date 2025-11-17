package pgxadapter

import "github.com/casbin/casbin/v2/model"

// FilteredAdapter is the interface for Casbin adapters supporting filtered policies.
type FilteredAdapter interface {
	Adapter

	// LoadFilteredPolicy loads only policy rules that match the filter.
	LoadFilteredPolicy(model model.Model, filter any) error
	// IsFiltered returns true if the loaded policy has been filtered.
	IsFiltered() bool
}

package pgxadapter

import (
	"context"

	"github.com/casbin/casbin/v2/model"
)

// FilteredAdapter is the interface for Casbin adapters supporting filtered policies.
type FilteredAdapter interface {
	Adapter

	// LoadFilteredPolicy loads only policy rules that match the filter.
	LoadFilteredPolicy(model model.Model, filter any) error
	// IsFiltered returns true if the loaded policy has been filtered.
	IsFiltered() bool
}

// ContextFilteredAdapter is the context-aware interface for Casbin adapters supporting filtered policies.
type ContextFilteredAdapter interface {
	ContextAdapter

	// LoadFilteredPolicyCtx loads only policy rules that match the filter.
	LoadFilteredPolicyCtx(ctx context.Context, model model.Model, filter any) error
	// IsFilteredCtx returns true if the loaded policy has been filtered.
	IsFilteredCtx(ctx context.Context) bool
}

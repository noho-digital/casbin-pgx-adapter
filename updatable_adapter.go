package pgxadapter

import "context"

// UpdatableAdapter is the interface for Casbin adapters with add update policy function.
type UpdatableAdapter interface {
	Adapter
	// UpdatePolicy updates a policy rule from storage.
	// This is part of the Auto-Save feature.
	UpdatePolicy(sec string, ptype string, oldRule, newRule []string) error
	// UpdatePolicies updates some policy rules to storage, like db, redis.
	UpdatePolicies(sec string, ptype string, oldRules, newRules [][]string) error
	// UpdateFilteredPolicies deletes old rules and adds new rules.
	UpdateFilteredPolicies(sec string, ptype string, newRules [][]string, fieldIndex int, fieldValues ...string) ([][]string, error)
}

type ContextUpdatableAdapter interface {
	ContextAdapter

	// UpdatePolicyCtx updates a policy rule from storage.
	// This is part of the Auto-Save feature.
	UpdatePolicyCtx(ctx context.Context, sec string, ptype string, oldRule, newRule []string) error
	// UpdatePoliciesCtx updates some policy rules to storage, like db, redis.
	UpdatePoliciesCtx(ctx context.Context, sec string, ptype string, oldRules, newRules [][]string) error
	// UpdateFilteredPoliciesCtx deletes old rules and adds new rules.
	UpdateFilteredPoliciesCtx(ctx context.Context, sec string, ptype string, newRules [][]string, fieldIndex int, fieldValues ...string) ([][]string, error)
}

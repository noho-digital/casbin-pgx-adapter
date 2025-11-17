package pgxadapter

import "context"

// AddPolicies adds policy rules to the storage
func (a *PgxAdapter) AddPolicies(sec string, ptype string, rules [][]string) error {
	return a.AddPoliciesCtx(context.Background(), sec, ptype, rules)
}

// RemovePolicies removes policy rules from the storage
func (a *PgxAdapter) RemovePolicies(sec string, ptype string, rules [][]string) error {
	return a.RemovePoliciesCtx(context.Background(), sec, ptype, rules)
}

package pgxadapter

import (
	"context"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

// AddPolicies adds policy rules to the storage
func (a *PgxAdapter) AddPolicies(sec string, ptype string, rules [][]string) error {
	return a.AddPoliciesCtx(context.Background(), sec, ptype, rules)
}

// RemovePolicies removes policy rules from the storage
func (a *PgxAdapter) RemovePolicies(sec string, ptype string, rules [][]string) error {
	return a.RemovePoliciesCtx(context.Background(), sec, ptype, rules)
}

// AddPolicies adds policy rules to the storage
func (a *PgxAdapter) AddPoliciesCtx(ctx context.Context, sec string, ptype string, rules [][]string) error {
	if len(rules) == 0 {
		return nil
	}

	insertBuilder := a.psql.Insert(a.tableName).
		Columns(insertColumns...).
		Suffix("ON CONFLICT DO NOTHING")

	for _, rule := range rules {
		vals := make([]any, 7)
		vals[0] = ptype

		for i := range 6 {
			if i < len(rule) && rule[i] != "" {
				vals[i+1] = rule[i]
			} else {
				vals[i+1] = nil
			}
		}

		insertBuilder = insertBuilder.Values(vals...)
	}

	sqlStr, args, err := insertBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build insert query: %w", err)
	}

	_, err = a.db.ExecContext(ctx, sqlStr, args...)

	if err != nil {
		return fmt.Errorf("failed to add policies: %w", err)
	}

	return nil
}

// RemovePolicies removes policy rules from the storage
func (a *PgxAdapter) RemovePoliciesCtx(ctx context.Context, sec string, ptype string, rules [][]string) error {
	if len(rules) == 0 {
		return nil
	}

	// Start a transaction
	tx, err := a.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	for _, rule := range rules {
		deleteBuilder := a.psql.Delete(a.tableName).Where(sq.Eq{"ptype": ptype})

		// Add conditions for each rule value
		for i := range 6 {
			col := colParams[i]
			if i < len(rule) && rule[i] != "" {
				deleteBuilder = deleteBuilder.Where(sq.Eq{col: rule[i]})
			}
		}

		sqlStr, args, err := deleteBuilder.ToSql()
		if err != nil {
			return fmt.Errorf("failed to build delete query: %w", err)
		}

		_, err = tx.ExecContext(ctx, sqlStr, args...)
		if err != nil {
			return fmt.Errorf("failed to remove policy: %w", err)
		}

	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

package pgxadapter

import (
	"context"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/jackc/pgx/v5/pgconn"
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
		Columns(insertColumns...)

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

	sql, args, err := insertBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build insert query: %w", err)
	}

	var result pgconn.CommandTag
	result, err = a.conn.Exec(ctx, sql, args...)

	if err != nil {
		// Check if it's a unique constraint violation
		if strings.Contains(err.Error(), "duplicate key") {
			return fmt.Errorf("one or more policies already exist")
		}
		return fmt.Errorf("failed to add policies: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("no rows affected")
	}

	return nil
}

// RemovePolicies removes policy rules from the storage
func (a *PgxAdapter) RemovePoliciesCtx(ctx context.Context, sec string, ptype string, rules [][]string) error {
	if len(rules) == 0 {
		return nil
	}

	// Start a transaction
	tx, err := a.conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	var totalRowsAffected int64

	for _, rule := range rules {
		deleteBuilder := a.psql.Delete(a.tableName).Where(sq.Eq{"ptype": ptype})

		// Add conditions for each rule value
		for i := range 6 {
			col := colParams[i]
			if i < len(rule) && rule[i] != "" {
				deleteBuilder = deleteBuilder.Where(sq.Eq{col: rule[i]})
			} else {
				deleteBuilder = deleteBuilder.Where(sq.Eq{col: nil})
			}
		}

		sql, args, err := deleteBuilder.ToSql()
		if err != nil {
			return fmt.Errorf("failed to build delete query: %w", err)
		}

		var result pgconn.CommandTag
		result, err = tx.Exec(ctx, sql, args...)

		if err != nil {
			return fmt.Errorf("failed to remove policy: %w", err)
		}

		totalRowsAffected += result.RowsAffected()
	}

	if totalRowsAffected == 0 {
		return fmt.Errorf("no policies found")
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

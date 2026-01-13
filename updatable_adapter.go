package pgxadapter

import (
	"context"
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
)

// UpdatePolicy updates a policy rule from storage
func (a *PgxAdapter) UpdatePolicy(sec string, ptype string, oldRule, newRule []string) error {
	return a.UpdatePolicyCtx(context.Background(), sec, ptype, oldRule, newRule)
}

// UpdatePolicies updates multiple policy rules in storage
func (a *PgxAdapter) UpdatePolicies(sec string, ptype string, oldRules, newRules [][]string) error {
	return a.UpdatePoliciesCtx(context.Background(), sec, ptype, oldRules, newRules)
}

// UpdateFilteredPolicies deletes old rules matching the filter and adds new rules
func (a *PgxAdapter) UpdateFilteredPolicies(sec string, ptype string, newRules [][]string, fieldIndex int, fieldValues ...string) ([][]string, error) {
	return a.UpdateFilteredPoliciesCtx(context.Background(), sec, ptype, newRules, fieldIndex, fieldValues...)
}

// UpdatePolicyCtx updates a policy rule from storage
func (a *PgxAdapter) UpdatePolicyCtx(ctx context.Context, sec string, ptype string, oldRule, newRule []string) error {
	// Build WHERE clause for old rule
	updateBuilder := a.psql.Update(a.tableName).Where(sq.Eq{"ptype": ptype})

	// Add conditions for each old rule value
	for i := range 6 {
		col := colParams[i]
		if i < len(oldRule) && oldRule[i] != "" {
			updateBuilder = updateBuilder.Where(sq.Eq{col: oldRule[i]})
		} else {
			updateBuilder = updateBuilder.Where(sq.Eq{col: nil})
		}
	}

	// Build SET clause for new rule
	setMap := make(map[string]any)
	for i := range 6 {
		col := colParams[i]
		if i < len(newRule) && newRule[i] != "" {
			setMap[col] = newRule[i]
		} else {
			setMap[col] = nil
		}
	}
	updateBuilder = updateBuilder.SetMap(setMap)

	sqlQuery, args, err := updateBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build update query: %w", err)
	}

	result, err := a.db.Exec(ctx, sqlQuery, args...)
	if err != nil {
		return fmt.Errorf("failed to update policy: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("policy not found")
	}

	return nil
}

// UpdatePoliciesCtx updates multiple policy rules in storage within a transaction
func (a *PgxAdapter) UpdatePoliciesCtx(ctx context.Context, sec string, ptype string, oldRules, newRules [][]string) error {
	if len(oldRules) != len(newRules) {
		return fmt.Errorf("old rules and new rules must have the same length")
	}

	if len(oldRules) == 0 {
		return nil
	}

	tx, err := a.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	for i := range oldRules {
		oldRule := oldRules[i]
		newRule := newRules[i]

		// Build WHERE clause for old rule
		updateBuilder := a.psql.Update(a.tableName).Where(sq.Eq{"ptype": ptype})

		// Add conditions for each old rule value
		for j := range 6 {
			col := colParams[j]
			if j < len(oldRule) && oldRule[j] != "" {
				updateBuilder = updateBuilder.Where(sq.Eq{col: oldRule[j]})
			} else {
				updateBuilder = updateBuilder.Where(sq.Eq{col: nil})
			}
		}

		// Build SET clause for new rule
		setMap := make(map[string]any)
		for j := range 6 {
			col := colParams[j]
			if j < len(newRule) && newRule[j] != "" {
				setMap[col] = newRule[j]
			} else {
				setMap[col] = nil
			}
		}
		updateBuilder = updateBuilder.SetMap(setMap)

		sqlQuery, args, err := updateBuilder.ToSql()
		if err != nil {
			return fmt.Errorf("failed to build update query: %w", err)
		}

		result, err := tx.Exec(ctx, sqlQuery, args...)
		if err != nil {
			return fmt.Errorf("failed to update policy: %w", err)
		}

		if result.RowsAffected() == 0 {
			return fmt.Errorf("policy not found at index %d", i)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// UpdateFilteredPoliciesCtx deletes old rules matching the filter and adds new rules
func (a *PgxAdapter) UpdateFilteredPoliciesCtx(ctx context.Context, sec string, ptype string, newRules [][]string, fieldIndex int, fieldValues ...string) ([][]string, error) {
	if fieldIndex < 0 || fieldIndex > 5 {
		return nil, fmt.Errorf("invalid field index: %d", fieldIndex)
	}

	tx, err := a.db.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Build query to find matching old policies
	selectBuilder := a.psql.Select(selectColumns...).From(a.tableName).Where(sq.Eq{"ptype": ptype})

	// Add filter conditions
	for i := range fieldValues {
		if i+fieldIndex > 5 {
			break
		}
		col := colParams[i+fieldIndex]
		selectBuilder = selectBuilder.Where(sq.Eq{col: fieldValues[i]})
	}

	sqlQuery, args, err := selectBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build select query: %w", err)
	}

	rows, err := tx.Query(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query policies: %w", err)
	}

	var oldPolicies [][]string
	for rows.Next() {
		var ptypeVal string
		var v0, v1, v2, v3, v4, v5 sql.NullString

		if err := rows.Scan(&ptypeVal, &v0, &v1, &v2, &v3, &v4, &v5); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		policy := []string{}
		if v0.Valid {
			policy = append(policy, v0.String)
		}
		if v1.Valid {
			policy = append(policy, v1.String)
		}
		if v2.Valid {
			policy = append(policy, v2.String)
		}
		if v3.Valid {
			policy = append(policy, v3.String)
		}
		if v4.Valid {
			policy = append(policy, v4.String)
		}
		if v5.Valid {
			policy = append(policy, v5.String)
		}

		oldPolicies = append(oldPolicies, policy)
	}
	rows.Close()

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	// Delete old policies matching the filter
	deleteBuilder := a.psql.Delete(a.tableName).Where(sq.Eq{"ptype": ptype})
	for i := range fieldValues {
		if i+fieldIndex > 5 {
			break
		}
		col := colParams[i+fieldIndex]
		deleteBuilder = deleteBuilder.Where(sq.Eq{col: fieldValues[i]})
	}

	sqlQuery, args, err = deleteBuilder.ToSql()
	if err != nil {
		return nil, fmt.Errorf("failed to build delete query: %w", err)
	}

	_, err = tx.Exec(ctx, sqlQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to delete policies: %w", err)
	}

	// Insert new policies
	if len(newRules) > 0 {
		insertBuilder := a.psql.Insert(a.tableName).Columns(insertColumns...)

		for _, rule := range newRules {
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

		sqlQuery, args, err = insertBuilder.ToSql()
		if err != nil {
			return nil, fmt.Errorf("failed to build insert query: %w", err)
		}

		_, err = tx.Exec(ctx, sqlQuery, args...)
		if err != nil {
			return nil, fmt.Errorf("failed to insert new policies: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return oldPolicies, nil
}

package pgxadapter

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	sq "github.com/Masterminds/squirrel"
	"github.com/casbin/casbin/v3/model"
	"github.com/casbin/casbin/v3/persist"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// LoadPolicy loads all policy rules from the storage
func (a *PgxAdapter) LoadPolicy(model model.Model) error {
	return a.LoadPolicyCtx(context.Background(), model)
}

// SavePolicy saves all policy rules to the storage
func (a *PgxAdapter) SavePolicy(model model.Model) error {
	return a.SavePolicyCtx(context.Background(), model)
}

// AddPolicy adds a policy rule to the storage
func (a *PgxAdapter) AddPolicy(sec string, ptype string, rule []string) error {
	return a.AddPolicyCtx(context.Background(), sec, ptype, rule)
}

// RemovePolicy removes a policy rule from the storage
func (a *PgxAdapter) RemovePolicy(sec string, ptype string, rule []string) error {
	return a.RemovePolicyCtx(context.Background(), sec, ptype, rule)
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage
func (a *PgxAdapter) RemoveFilteredPolicy(sec string, ptype string, fieldIndex int, fieldValues ...string) error {
	return a.RemoveFilteredPolicyCtx(context.Background(), sec, ptype, fieldIndex, fieldValues...)
}

// LoadPolicy loads all policy rules from the storage
func (a *PgxAdapter) LoadPolicyCtx(ctx context.Context, model model.Model) error {

	q, args, err := a.psql.
		Select(selectColumns...).
		From(a.tableName).
		OrderBy("id").
		ToSql()

	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	rows, err := a.db.Query(ctx, q, args...)

	if err != nil {
		return fmt.Errorf("failed to query policies: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ptype string
		var v0, v1, v2, v3, v4, v5 sql.NullString

		if err := rows.Scan(&ptype, &v0, &v1, &v2, &v3, &v4, &v5); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		policyLine := []string{ptype}

		if v0.Valid {
			policyLine = append(policyLine, v0.String)
		}
		if v1.Valid {
			policyLine = append(policyLine, v1.String)
		}
		if v2.Valid {
			policyLine = append(policyLine, v2.String)
		}
		if v3.Valid {
			policyLine = append(policyLine, v3.String)
		}
		if v4.Valid {
			policyLine = append(policyLine, v4.String)
		}
		if v5.Valid {
			policyLine = append(policyLine, v5.String)
		}

		persist.LoadPolicyLine(strings.Join(policyLine, ", "), model)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return nil
}

// SavePolicy saves all policy rules to the storage
func (a *PgxAdapter) SavePolicyCtx(ctx context.Context, model model.Model) error {

	// Start a transaction
	tx, err := a.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Clear existing policies
	quotedTableName := pgx.Identifier{a.tableName}.Sanitize()
	truncateSQL := "TRUNCATE TABLE " + quotedTableName
	if _, err := tx.Exec(ctx, truncateSQL); err != nil {
		return fmt.Errorf("failed to clear policies: %w", err)
	}

	// Prepare batch insert
	var lines [][]string
	var ptypes []string

	// Collect all policy lines
	for ptype, ast := range model["p"] {
		for _, rule := range ast.Policy {
			lines = append(lines, rule)
			ptypes = append(ptypes, ptype)
		}
	}

	for ptype, ast := range model["g"] {
		for _, rule := range ast.Policy {
			lines = append(lines, rule)
			ptypes = append(ptypes, ptype)
		}
	}

	// Batch insert all policies
	if len(lines) > 0 {
		insertBuilder := a.psql.Insert(a.tableName).
			Columns(insertColumns...)

		for i, line := range lines {
			vals := make([]any, 7)
			vals[0] = ptypes[i]

			for j := range 6 {
				if j < len(line) && line[j] != "" {
					vals[j+1] = line[j]
				} else {
					vals[j+1] = nil
				}
			}

			insertBuilder = insertBuilder.Values(vals...)
		}

		sql, args, err := insertBuilder.ToSql()
		if err != nil {
			return fmt.Errorf("failed to build insert query: %w", err)
		}

		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			return fmt.Errorf("failed to insert policies: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// AddPolicy adds a policy rule to the storage
func (a *PgxAdapter) AddPolicyCtx(ctx context.Context, sec string, ptype string, rule []string) error {

	vals := make([]any, 7)
	vals[0] = ptype

	for i := range 6 {
		if i < len(rule) && rule[i] != "" {
			vals[i+1] = rule[i]
		} else {
			vals[i+1] = nil
		}
	}

	sql, args, err := a.psql.
		Insert(a.tableName).
		Columns(insertColumns...).
		Values(vals...).
		ToSql()

	if err != nil {
		return fmt.Errorf("failed to build insert query: %w", err)
	}

	var result pgconn.CommandTag
	result, err = a.db.Exec(ctx, sql, args...)

	if err != nil {
		// Check if it's a unique constraint violation
		if strings.Contains(err.Error(), "duplicate key") {
			return fmt.Errorf("policy already exists")
		}
		return fmt.Errorf("failed to add policy: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("no rows affected")
	}

	return nil
}

// RemovePolicy removes a policy rule from the storage
func (a *PgxAdapter) RemovePolicyCtx(ctx context.Context, sec string, ptype string, rule []string) error {

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
	result, err = a.db.Exec(ctx, sql, args...)

	if err != nil {
		return fmt.Errorf("failed to remove policy: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("policy not found")
	}

	return nil
}

// RemoveFilteredPolicy removes policy rules that match the filter from the storage
func (a *PgxAdapter) RemoveFilteredPolicyCtx(ctx context.Context, sec string, ptype string, fieldIndex int, fieldValues ...string) error {

	if fieldIndex < 0 || fieldIndex > 5 {
		return fmt.Errorf("invalid field index: %d", fieldIndex)
	}

	deleteBuilder := a.psql.Delete(a.tableName).Where(sq.Eq{"ptype": ptype})

	// Add conditions for filtered values
	for i := range fieldValues {
		if i+fieldIndex > 5 {
			break
		}
		col := colParams[i+fieldIndex]
		if fieldValues[i] != "" {
			deleteBuilder = deleteBuilder.Where(sq.Eq{col: fieldValues[i]})
		}
	}

	sql, args, err := deleteBuilder.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build delete query: %w", err)
	}

	var result pgconn.CommandTag
	result, err = a.db.Exec(ctx, sql, args...)

	if err != nil {
		return fmt.Errorf("failed to remove filtered policies: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("no matching policies found")
	}

	return nil
}

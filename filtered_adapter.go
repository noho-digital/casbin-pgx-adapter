package pgxadapter

import (
	"context"
	"database/sql"
	"fmt"

	sq "github.com/Masterminds/squirrel"
	"github.com/casbin/casbin/v3/model"
	"github.com/casbin/casbin/v3/persist"
)

// Filter defines the filtering rules for a FilteredAdapter's policy.
// Empty values are ignored, but all others must match the filter.
type Filter struct {
	Ptype []string
	V0    []string
	V1    []string
	V2    []string
	V3    []string
	V4    []string
	V5    []string
}

// BatchFilter wraps multiple filters for OR-based filtering.
// Each filter in the batch is applied separately, and results are combined.
type BatchFilter struct {
	Filters []Filter
}

// LoadFilteredPolicy loads only policy rules that match the filter
func (a *PgxAdapter) LoadFilteredPolicy(model model.Model, filter any) error {
	return a.LoadFilteredPolicyCtx(context.Background(), model, filter)
}

// IsFiltered returns true if the loaded policy has been filtered
func (a *PgxAdapter) IsFiltered() bool {
	return a.IsFilteredCtx(context.Background())
}

// LoadFilteredPolicyCtx loads only policy rules that match the filter.
// Supports Filter for single filter or BatchFilter for OR-based filtering.
func (a *PgxAdapter) LoadFilteredPolicyCtx(ctx context.Context, model model.Model, filter any) error {
	if filter == nil {
		a.mu.Lock()
		a.isFiltered = false
		a.mu.Unlock()
		return a.LoadPolicyCtx(ctx, model)
	}

	var filters []Filter
	switch f := filter.(type) {
	case Filter:
		filters = []Filter{f}
	case *Filter:
		filters = []Filter{*f}
	case BatchFilter:
		filters = f.Filters
	case *BatchFilter:
		filters = f.Filters
	case []Filter:
		filters = f
	default:
		return fmt.Errorf("invalid filter type")
	}

	a.mu.Lock()
	a.isFiltered = true
	a.mu.Unlock()

	for _, filterValue := range filters {
		if err := a.loadFilteredPolicies(ctx, model, filterValue); err != nil {
			return err
		}
	}

	return nil
}

func (a *PgxAdapter) loadFilteredPolicies(ctx context.Context, model model.Model, filterValue Filter) error {
	query := a.psql.
		Select(selectColumns...).
		From(a.tableName).
		OrderBy("id")

	if len(filterValue.Ptype) > 0 {
		query = query.Where(sq.Eq{"ptype": filterValue.Ptype})
	}
	if len(filterValue.V0) > 0 {
		query = query.Where(sq.Eq{"v0": filterValue.V0})
	}
	if len(filterValue.V1) > 0 {
		query = query.Where(sq.Eq{"v1": filterValue.V1})
	}
	if len(filterValue.V2) > 0 {
		query = query.Where(sq.Eq{"v2": filterValue.V2})
	}
	if len(filterValue.V3) > 0 {
		query = query.Where(sq.Eq{"v3": filterValue.V3})
	}
	if len(filterValue.V4) > 0 {
		query = query.Where(sq.Eq{"v4": filterValue.V4})
	}
	if len(filterValue.V5) > 0 {
		query = query.Where(sq.Eq{"v5": filterValue.V5})
	}

	sqlQuery, args, err := query.ToSql()
	if err != nil {
		return fmt.Errorf("failed to build query: %w", err)
	}

	rows, err := a.db.Query(ctx, sqlQuery, args...)
	if err != nil {
		return fmt.Errorf("failed to query policies: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var ptypeVal string
		var v0, v1, v2, v3, v4, v5 sql.NullString

		if err := rows.Scan(&ptypeVal, &v0, &v1, &v2, &v3, &v4, &v5); err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}

		line := []string{ptypeVal}

		if v0.Valid {
			line = append(line, v0.String)
		}
		if v1.Valid {
			line = append(line, v1.String)
		}
		if v2.Valid {
			line = append(line, v2.String)
		}
		if v3.Valid {
			line = append(line, v3.String)
		}
		if v4.Valid {
			line = append(line, v4.String)
		}
		if v5.Valid {
			line = append(line, v5.String)
		}

		if err := persist.LoadPolicyArray(line, model); err != nil {
			return err
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating rows: %w", err)
	}

	return nil
}

// IsFilteredCtx returns true if the loaded policy has been filtered
func (a *PgxAdapter) IsFilteredCtx(ctx context.Context) bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.isFiltered
}

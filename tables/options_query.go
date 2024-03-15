package tables

import (
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// queryOption is a simple base type for providing query mutations
type queryOption struct {
	queryMutator   func(q *gocqlx.Queryx) *gocqlx.Queryx
	queryBuilderFn func(builder *qb.SelectBuilder) *qb.SelectBuilder
}

// applyToSelectBuilder applies this option to the given select builder
func (s *queryOption) applyToQuery(q *gocqlx.Queryx) *gocqlx.Queryx {
	if s.queryMutator == nil {
		return q
	}
	return s.queryMutator(q)
}

// applyToSelectBuilder applies this option to the given select builder
func (s *queryOption) applyToBuilder(builder *qb.SelectBuilder) *qb.SelectBuilder {
	if s.queryBuilderFn == nil {
		return builder
	}
	return s.queryBuilderFn(builder)
}

// WithPaging sets the paging state to enable resuming a query on a revisit
func WithPaging(pageSize int, state []byte) QueryOption {
	return &queryOption{
		queryMutator: func(q *gocqlx.Queryx) *gocqlx.Queryx {
			return q.PageSize(pageSize).PageState(state)
		},
	}
}

// WithSort sets the sort order for a query result
func WithSort(column string, order int) QueryOption {
	return &queryOption{
		queryBuilderFn: func(builder *qb.SelectBuilder) *qb.SelectBuilder {
			if order == 0 || column == "" {
				return builder
			}
			return builder.OrderBy(column, order > 0)
		},
	}
}

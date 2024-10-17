package tables

import (
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// queryOption is a simple base type for providing query mutations
type queryOption struct {
	queryMutator   func(q *gocqlx.Queryx) *gocqlx.Queryx
	queryBuilderFn func(builder *qb.SelectBuilder) *qb.SelectBuilder
	queryColumns   []string
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

func (s *queryOption) columns() []string {
	return s.queryColumns
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

// WithColumns specifies the columns to return in a query result
func WithColumns(columns ...string) QueryOption {
	return &queryOption{
		queryColumns: columns,
	}
}

// WithPredicates specifies the columns to test against in a query.
// This must be paired with a `WithBindings` call to match the specific values in the test
func WithPredicates(predicates ...qb.Cmp) QueryOption {
	return &queryOption{
		queryBuilderFn: func(builder *qb.SelectBuilder) *qb.SelectBuilder {
			if len(predicates) == 0 {
				return builder
			}
			return builder.Where(predicates...)
		},
	}
}

// WithBindings specifies the values of keys to query against (ie the bound values of keys in a `where` clause)
// This must be paired with a `WithPredicates` call to match the column names that the values relate to
func WithBindings(keys ...any) QueryOption {
	return &queryOption{
		queryMutator: func(query *gocqlx.Queryx) *gocqlx.Queryx {
			if len(keys) == 0 {
				return query
			}
			return query.Bind(keys...)
		},
	}
}

// WithKey creates a query option that translates to a `name = value` statement in a `where` clause.
// Note, don't use inbetween a WithPredicates and WithBindings option - that will mess up key -> value alignment
func WithKey(name string, value any) QueryOption {
	return &queryOption{
		queryBuilderFn: func(builder *qb.SelectBuilder) *qb.SelectBuilder {
			return builder.Where(qb.Eq(name))
		},
		queryMutator: func(query *gocqlx.Queryx) *gocqlx.Queryx {
			return query.Bind(value)
		},
	}
}

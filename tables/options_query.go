package tables

import (
	"github.com/scylladb/gocqlx/v2"
)

// queryOption is a simple base type for providing query mutations
type queryOption struct {
	queryMutator func(q *gocqlx.Queryx) *gocqlx.Queryx
}

// applyToSelectBuilder applies this option to the given select builder
func (s *queryOption) applyToQuery(q *gocqlx.Queryx) *gocqlx.Queryx {
	if s.queryMutator == nil {
		return q
	}

	return s.queryMutator(q)
}

// WithPaging sets the paging state to enable resuming a query on a revisit
func WithPaging(pageSize int, state []byte) QueryOption {
	return &queryOption{
		queryMutator: func(q *gocqlx.Queryx) *gocqlx.Queryx {
			return q.PageSize(pageSize).PageState(state)
		},
	}
}

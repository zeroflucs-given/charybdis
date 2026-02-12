package tables

import (
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

// deleteOption is a simple base type for providing delete mutations
type deleteOption struct {
	mutator            func(q *gocqlx.Queryx) *gocqlx.Queryx
	targetColumns      []string
	targetPredicates   []qb.Cmp
	targetBindings     []any
	targetIfConditions []qb.Cmp
	targetExists       bool
}

// applyToSelectBuilder applies this option to the given select builder
func (s *deleteOption) applyToQuery(q *gocqlx.Queryx) *gocqlx.Queryx {
	if s.mutator == nil {
		return q
	}
	return s.mutator(q)
}

func (s *deleteOption) columns() []string {
	return s.targetColumns
}

func (s *deleteOption) predicates() []qb.Cmp {
	return s.targetPredicates
}

func (s *deleteOption) bindings() []any {
	return s.targetBindings
}

func (s *deleteOption) ifConditions() []qb.Cmp {
	return s.targetIfConditions
}

func (s *deleteOption) ifExists() bool {
	return s.targetExists
}

// DeleteColumns specifies the columns to delete from matched rows
func DeleteColumns(columns ...string) DeleteOption {
	return &deleteOption{
		targetColumns: columns,
	}
}

// WithDeletePredicates specifies the columns to test against in a query.
// This must be paired with a `WithDeletionBindings` call to match the specific values in the test
func WithDeletePredicates(predicates ...qb.Cmp) DeleteOption {
	return &deleteOption{
		targetPredicates: predicates,
	}
}

// WithDeletionBindings specifies the values of keys to query against (ie the bound values of keys in a `where` clause)
// This must be paired with a `WithDeletePredicates` call to match the column names that the values relate to
func WithDeletionBindings(bindings ...any) DeleteOption {
	return &deleteOption{
		targetBindings: bindings,
	}
}

// WithDeletionKey creates a query option that translates to a `name = value` statement in a `where` clause.
// Note: don't use in between a WithPredicates and WithBindings option - that will mess up key -> value alignment
func WithDeletionKey(name string, value any) DeleteOption {
	return &deleteOption{
		targetPredicates: []qb.Cmp{qb.Eq(name)},
		targetBindings:   []any{value},
	}
}

// WithDeletionCondition creates a query option that translates to a `name op value` statement in a `where` clause.
// Note, don't use in between a WithDeletePredicates and WithDeletionBindings option - that will mess up key -> value alignment
func WithDeletionCondition(cond qb.Cmp, value any) DeleteOption {
	return &deleteOption{
		targetPredicates: []qb.Cmp{cond},
		targetBindings:   []any{value},
	}
}

func WithDeleteIf(cond qb.Cmp, value any) DeleteOption {
	return &deleteOption{
		targetIfConditions: []qb.Cmp{cond},
		targetBindings:     []any{value},
	}
}

func WithDeleteIfExists() DeleteOption {
	return &deleteOption{
		targetExists: true,
	}
}

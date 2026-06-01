package tables

import (
	"slices"
	"time"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

// deleteOption is a simple base type for providing delete mutations
type deleteOption struct {
	mutator        func(q *gocqlx.Queryx) *gocqlx.Queryx
	builderFn      func(builder *qb.DeleteBuilder) *qb.DeleteBuilder
	predicates     []qb.Cmp
	targetBindings []any
	isLWT          bool
}

// deleteOption applies this option to the given delete builder
func (s *deleteOption) applyToBuilder(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
	if s.builderFn == nil {
		return builder
	}
	return s.builderFn(builder)
}

// applyToQuery applies this option to the given select query
func (s *deleteOption) applyToQuery(q *gocqlx.Queryx) *gocqlx.Queryx {
	if s.mutator == nil {
		return q
	}
	return s.mutator(q)
}

func (s *deleteOption) conditions() []qb.Cmp {
	return s.predicates
}

func (s *deleteOption) bindings() []any {
	return s.targetBindings
}

func (s *deleteOption) isPrecondition() bool {
	return s.isLWT
}

// DeleteColumns specifies the columns to delete from matched rows
func DeleteColumns(columns ...string) DeleteOption {
	return &deleteOption{
		builderFn: func(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
			return builder.Columns(columns...)
		},
	}
}

// WithDeletePredicates specifies the columns to test against in a query.
// This must be paired with a `WithDeletionBindings` call to match the specific values in the test
func WithDeletePredicates(predicates ...qb.Cmp) DeleteOption {
	return &deleteOption{
		builderFn: func(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
			return builder.Where(predicates...)
		},
		predicates: slices.Clone(predicates),
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
		builderFn: func(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
			return builder.Where(qb.Eq(name))
		},
		targetBindings: []any{value},
	}
}

// WithDeletionCondition creates a query option that translates to a `name op value` statement in a `where` clause.
// Note, don't use in between a WithDeletePredicates and WithDeletionBindings option - that will mess up key -> value alignment
func WithDeletionCondition(cond qb.Cmp, value any) DeleteOption {
	return &deleteOption{
		builderFn: func(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
			return builder.Where(cond)
		},
		targetBindings: []any{value},
		predicates:     []qb.Cmp{cond},
	}
}

func WithDeleteIf(cond qb.Cmp, value any) DeleteOption {
	return &deleteOption{
		builderFn: func(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
			return builder.If(cond)
		},
		targetBindings: []any{value},
		predicates:     []qb.Cmp{cond},
		isLWT:          true,
	}
}

func WithDeleteIfExists() DeleteOption {
	return &deleteOption{
		builderFn: func(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
			return builder.Existing()
		},
		isLWT: true,
	}
}

func WithDeleteUsingTimestamp(ts int64) DeleteOption {
	return &deleteOption{
		builderFn: func(builder *qb.DeleteBuilder) *qb.DeleteBuilder {
			return builder.Timestamp(time.UnixMilli(ts))
		},
	}
}

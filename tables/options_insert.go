package tables

import (
	"github.com/scylladb/gocqlx/v2/qb"
)

type insertOption struct {
	insertBuilderFn func(builder *qb.InsertBuilder) *qb.InsertBuilder
}

// Apply applies the update optionInsertBuilder
func (u *insertOption) applyToInsertBuilder(builder *qb.InsertBuilder) *qb.InsertBuilder {
	return u.insertBuilderFn(builder)
}

// WithNotExists sets IF NOT EXISTS on the query to ensure an insert is a new record.
func WithNotExists() InsertOption {
	return &insertOption{
		insertBuilderFn: func(builder *qb.InsertBuilder) *qb.InsertBuilder {
			return builder.Unique()
		},
	}
}

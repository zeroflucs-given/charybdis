package tables

import (
	"github.com/scylladb/gocqlx/v2/qb"
	"time"
)

type insertOption struct {
	insertBuilderFn   func(builder *qb.InsertBuilder) *qb.InsertBuilder
	isOptPrecondition bool
}

// Apply applies the update optionInsertBuilder
func (u *insertOption) applyToInsertBuilder(builder *qb.InsertBuilder) *qb.InsertBuilder {
	return u.insertBuilderFn(builder)
}

func (u *insertOption) isPrecondition() bool {
	return u.isOptPrecondition
}

// WithNotExists sets IF NOT EXISTS on the query to ensure an insert is a new record.
func WithNotExists() InsertOption {
	return &insertOption{
		insertBuilderFn: func(builder *qb.InsertBuilder) *qb.InsertBuilder {
			return builder.Unique()
		},
		isOptPrecondition: false,
	}
}

// WithInsertTTL sets the TTL option for an insert.
func WithInsertTTL(d time.Duration) InsertOption {
	return &insertOption{
		insertBuilderFn: func(builder *qb.InsertBuilder) *qb.InsertBuilder {
			return builder.TTL(d)
		},
	}
}

package tables

import (
	"time"

	"github.com/scylladb/gocqlx/v2/qb"
)

type upsertOption struct {
	insertBuilderFn   func(builder *qb.InsertBuilder) *qb.InsertBuilder
	updateBuilderFn   func(builder *qb.UpdateBuilder) *qb.UpdateBuilder
	isOptPrecondition bool
}

// Apply applies the update optionInsertBuilder
func (u *upsertOption) applyToInsertBuilder(builder *qb.InsertBuilder) *qb.InsertBuilder {
	if u.insertBuilderFn == nil {
		return builder
	}
	return u.insertBuilderFn(builder)
}

// Apply applies the update optionInsertBuilder
func (u *upsertOption) applyToUpdateBuilder(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
	if u.updateBuilderFn == nil {
		return builder
	}
	return u.updateBuilderFn(builder)
}

func (u *upsertOption) isPrecondition() bool {
	return u.isOptPrecondition
}

func (u *upsertOption) getMapData() map[string]interface{} {
	return nil
}

// WithTTL sets the TTL option for an upsert.
func WithTTL(d time.Duration) UpsertOption {
	return &upsertOption{
		insertBuilderFn: func(builder *qb.InsertBuilder) *qb.InsertBuilder {
			return builder.TTL(d)
		},
		updateBuilderFn: func(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
			return builder.TTL(d)
		},
		isOptPrecondition: false,
	}
}

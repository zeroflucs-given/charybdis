package tables

import (
	"time"

	"github.com/scylladb/gocqlx/v2/qb"
)

type upsertOption struct {
	mapData           map[string]any
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

func (u *upsertOption) getMapData() map[string]any {
	return u.mapData
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

// WithSimpleUpsertIf allows for a LWT that does a simple value-based comparison on a single column
func WithSimpleUpsertIf(targetColumn string, val any) UpsertOption {
	// Just needs to be a unique column name that won't be part of the
	// table specification. If someone uses this, queries will naturally fail.
	const simpleIfName = "charybdis_if"

	return &upsertOption{
		mapData: map[string]any{
			simpleIfName: val,
		},
		updateBuilderFn: func(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
			return builder.If(qb.EqNamed(targetColumn, simpleIfName))
		},
		isOptPrecondition: true,
	}
}

// WithConditionalUpsert does a conditional update with a custom predicate and many values.
func WithConditionalUpsert(cmp qb.Cmp, payload map[string]any) UpsertOption {
	return &upsertOption{
		mapData: payload,
		updateBuilderFn: func(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
			return builder.If(cmp)
		},
		isOptPrecondition: true,
	}
}

// WithUpsertExists sets IF EXISTS on the query to ensure the row being updated exists
func WithUpsertExists() UpsertOption {
	return &upsertOption{
		updateBuilderFn: func(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
			return builder.Existing()
		},
		isOptPrecondition: true,
	}
}

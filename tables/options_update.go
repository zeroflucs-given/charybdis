package tables

import (
	"github.com/scylladb/gocqlx/v2/qb"
)

// updateOption is our internal type that implements the core of the updateOption
// handling.
type updateOption struct {
	mapData           map[string]any
	updateBuilderFn   func(builder *qb.UpdateBuilder) *qb.UpdateBuilder
	isOptPrecondition bool
}

// Apply applies the update optionInsertBuilder
func (u *updateOption) applyToUpdateBuilder(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
	return u.updateBuilderFn(builder)
}

func (u *updateOption) getMapData() map[string]any {
	return u.mapData
}

func (u *updateOption) isPrecondition() bool {
	return u.isOptPrecondition
}

// WithSimpleIf allows for a LWT that does a simple value-based comparison on a single column
func WithSimpleIf(targetColumn string, val any) UpdateOption {
	// Just needs to be a unique column name that won't be part of the
	// table specification. If someone uses this, queries will naturally
	// fail.
	const simpleIfName = "charybdis_if"

	return &updateOption{
		mapData: map[string]any{
			simpleIfName: val,
		},
		updateBuilderFn: func(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
			return builder.If(qb.EqNamed(targetColumn, simpleIfName))
		},
		isOptPrecondition: true,
	}
}

// WithConditionalUpdate does a conditional update with a custom predicate and many values.
func WithConditionalUpdate(cmp qb.Cmp, payload map[string]any) UpdateOption {
	return &updateOption{
		mapData: payload,
		updateBuilderFn: func(builder *qb.UpdateBuilder) *qb.UpdateBuilder {
			return builder.If(cmp)
		},
		isOptPrecondition: true,
	}
}

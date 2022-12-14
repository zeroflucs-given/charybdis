package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2/qb"
)

// Update updates an object. It will error if the object does not exist.
func (t *tableManagerImpl[T]) Update(ctx context.Context, instance *T, opts ...UpdateOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/Update", t.TraceAttributes, func(ctx context.Context) error {
		return t.updateInternal(ctx, instance, opts...)
	})
}

// updateInternal is a helper function that performs a single update
func (t *tableManagerImpl[T]) updateInternal(ctx context.Context, instance *T, opts ...UpdateOption) error {
	// Pre-change hooks
	errPre := t.runPreHooks(ctx, instance)
	if errPre != nil {
		return errPre
	}

	// Build our query
	query := qb.Update(t.qualifiedTableName).
		Set(t.nonKeyColumns...).
		Where(t.allKeyPredicates...)

	additionalVals := map[string]interface{}{}
	havePreconditions := false

	for _, opt := range opts {
		if opt.isPrecondition() {
			havePreconditions = true
		}
		query = opt.applyToUpdateBuilder(query)
		for k, v := range opt.getMapData() {
			additionalVals[k] = v
		}
	}

	// If we have no other preconditions, add an IF EXISTS check
	if !havePreconditions {
		query = query.
			Existing()
	}

	stmt, params := query.ToCql()
	applied, err := t.Session.ContextQuery(ctx, stmt, params).
		BindStructMap(instance, additionalVals).
		ExecCASRelease()
	if err != nil {
		return err
	} else if !applied {
		return ErrPreconditionFailed
	}

	// Post-change hooks
	errPost := t.runPostHooks(ctx, instance)
	if errPost != nil {
		return errPost
	}

	return nil
}

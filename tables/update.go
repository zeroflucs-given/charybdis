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
	// Build our query
	query := qb.Update(t.qualifiedTableName).
		Set(t.nonKeyColumns...).
		Where(t.allKeyPredicates...).
		Existing()

	additionalVals := map[string]interface{}{}

	for _, opt := range opts {
		query = opt.applyToUpdateBuilder(query)
		for k, v := range opt.getMapData() {
			additionalVals[k] = v
		}
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

	return nil
}

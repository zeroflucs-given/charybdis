package tables

import (
	"context"
)

// Delete removes an object by binding against the structure values. Technically only the
// keys of the object need be set.
func (t *tableManagerImpl[T]) Delete(ctx context.Context, instance *T) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/DeleteByObject", t.TraceAttributes, func(ctx context.Context) error {
		return t.Table.
			DeleteQueryContext(ctx, t.Session).
			Consistency(t.writeConsistency).
			BindStruct(instance).
			Exec()
	})
}

// DeleteByPrimaryKey remvoes a single row by primary key
func (t *tableManagerImpl[T]) DeleteByPrimaryKey(ctx context.Context, keys ...interface{}) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/DeleteByPrimaryKey", t.TraceAttributes, func(ctx context.Context) error {
		return t.Table.
			DeleteQueryContext(ctx, t.Session).
			Consistency(t.writeConsistency).
			Bind(keys...).
			Exec()
	})
}

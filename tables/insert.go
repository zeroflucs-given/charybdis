package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2/qb"
	"golang.org/x/sync/errgroup"
)

// Insert inserts a single object. Unlike upsert it enforces the value does not exist. You can achieve
// the same effect with an Upsert if you use the WithNotExist option.
func (t *tableManagerImpl[T]) Insert(ctx context.Context, instance *T, opts ...InsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Spec.Name+"/Insert", t.TraceAttributes, func(ctx context.Context) error {
		return t.insertInternal(ctx, instance, opts...)
	})
}

// InsertBulk inserts many objects in parallel, up to a given number. If the concurrency limit is not set,
// then a default of DefaultBulkConcurrency is used.
func (t *tableManagerImpl[T]) InsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...InsertOption) error {
	if concurrency <= 0 {
		concurrency = DefaultBulkConcurrency
	}

	return doWithTracing(ctx, t.Tracer, t.Spec.Name+"/InsertBulk", t.TraceAttributes, func(ctx context.Context) error {
		grp, grpCtx := errgroup.WithContext(ctx)
		grp.SetLimit(concurrency)

		for _, v := range instances {
			item := v
			grp.Go(func() error {
				return t.insertInternal(grpCtx, item, opts...)
			})
		}

		return grp.Wait()
	})
}

// insertInternal is a helper function that performs a single upsert
func (t *tableManagerImpl[T]) insertInternal(ctx context.Context, instance *T, opts ...InsertOption) error {
	// We must exist
	opts = append(opts, WithNotExists())

	// Build our query
	query := qb.Insert(t.qualifiedTableName).Columns(t.allColumnNames...)

	for _, opt := range opts {
		query = opt.applyToInsertBuilder(query)
	}

	stmt, params := query.ToCql()

	applied, err := t.Session.ContextQuery(ctx, stmt, params).
		Consistency(t.writeConsistency).
		BindStruct(instance).
		ExecCASRelease()

	if err != nil {
		return err
	}

	if !applied {
		return ErrPreconditionFailed
	}

	return nil
}

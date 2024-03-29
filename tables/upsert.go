package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2/qb"
	"golang.org/x/sync/errgroup"
)

// Upsert overwrites or inserts an object.
func (t *tableManagerImpl[T]) Upsert(ctx context.Context, instance *T, opts ...UpsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/Upsert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.upsertInternal(ctx, instance, opts...)
	})
}

// UpsertBulk upserts many objects in parallel, up to a given number. If the concurrency limit is not set,
// then a default of DefaultBulkConcurrency is used.
func (t *tableManagerImpl[T]) UpsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...UpsertOption) error {
	if concurrency <= 0 {
		concurrency = DefaultBulkConcurrency
	}

	return doWithTracing(ctx, t.Tracer, t.Name+"/UpsertBulk", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		grp, grpCtx := errgroup.WithContext(ctx)
		grp.SetLimit(concurrency)

		for _, v := range instances {
			item := v
			grp.Go(func() error {
				return t.upsertInternal(grpCtx, item, opts...)
			})
		}

		return grp.Wait()
	})
}

// upsertInternal is a helper function that performs a single upsert
func (t *tableManagerImpl[T]) upsertInternal(ctx context.Context, instance *T, opts ...UpsertOption) error {
	// Pre-change hooks
	errPre := t.runPreHooks(ctx, instance)
	if errPre != nil {
		return errPre
	}

	// Build our query
	query := qb.Update(t.qualifiedTableName).
		Set(t.nonKeyColumns...).
		Where(t.allKeyPredicates...)

	additionalVals := map[string]any{}

	for _, opt := range opts {
		query = opt.applyToUpdateBuilder(query)
		for k, v := range opt.getMapData() {
			additionalVals[k] = v
		}
	}

	stmt, params := query.ToCql()

	err := t.Session.ContextQuery(ctx, stmt, params).
		BindStructMap(instance, additionalVals).
		ExecRelease()

	if err != nil {
		return err
	}

	// Post-change hooks
	errPost := t.runPostHooks(ctx, instance)
	if errPost != nil {
		return errPost
	}

	return nil
}

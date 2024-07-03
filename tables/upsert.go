package tables

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2/qb"
	"go.uber.org/zap"
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

	st := time.Now()
	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()

	stmt, params := query.ToCql()
	for {
		err := t.Session.ContextQuery(retryCtx, stmt, params).
			BindStructMap(instance, additionalVals).
			ExecRelease()

		if err == nil {
			break
		}

		var wto *gocql.RequestErrWriteTimeout
		retryable := errors.As(err, &wto)
		if !retryable {
			return err
		}

		t.Logger.Debug("upsert retrying from early write timeout",
			zap.String("consistency", wto.Consistency.String()),
			zap.Int("received", wto.Received),
			zap.Int("blockFor", wto.BlockFor),
			zap.String("writeType", wto.WriteType),
			zap.Duration("set_timeout", t.queryTimeout),
			zap.Duration("execution_time_to_now", time.Since(st)),
		)
	}

	// Post-change hooks
	errPost := t.runPostHooks(ctx, instance)
	if errPost != nil {
		return errPost
	}

	return nil
}

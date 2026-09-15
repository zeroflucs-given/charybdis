package tables

import (
	"context"
	"errors"
	"maps"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Upsert overwrites or inserts an object.
func (t *tableManagerImpl[T]) Upsert(ctx context.Context, instance *T, opts ...UpsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name, "Upsert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.upsertInternal(ctx, instance, nil, opts...)
	})
}

// UpsertBulk upserts many objects in parallel, up to a given number. If the concurrency limit is not set,
// then a default of DefaultBulkConcurrency is used. The limit applies to statement executions only;
// preparation work (hooks, query building, binding) runs ahead on a bounded pool of workers.
func (t *tableManagerImpl[T]) UpsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...UpsertOption) error {
	if concurrency <= 0 {
		concurrency = DefaultBulkConcurrency
	}

	return doWithTracing(ctx, t.Tracer, t.Name, "UpsertBulk", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		gate := make(execGate, concurrency)
		grp, grpCtx := errgroup.WithContext(ctx)

		// Twice the gate size keeps preparation ahead of execution without a goroutine per instance
		var next atomic.Int64
		workers := min(len(instances), concurrency*2)
		for range workers {
			grp.Go(func() error {
				for {
					i := int(next.Add(1)) - 1
					if i >= len(instances) || grpCtx.Err() != nil {
						return nil
					}
					if err := t.upsertInternal(grpCtx, instances[i], gate, opts...); err != nil {
						return err
					}
				}
			})
		}

		return grp.Wait()
	})
}

// upsertInternal is a helper function that performs a single upsert
func (t *tableManagerImpl[T]) upsertInternal(ctx context.Context, instance *T, gate execGate, opts ...UpsertOption) error {
	// Pre-change hooks
	errPre := t.runPreHooks(ctx, instance)
	if errPre != nil {
		return errPre
	}

	var query *gocqlx.Queryx
	if len(opts) == 0 && t.codec != nil {
		query = t.Session.Query(t.upsertStmt, t.upsertNames).
			WithContext(ctx).
			Bind(t.codec.BindColumns(instance, t.upsertNames)...)
	} else if len(opts) == 0 {
		query = t.Session.Query(t.upsertStmt, t.upsertNames).
			WithContext(ctx).
			BindStruct(instance)
	} else {
		builder := qb.Update(t.qualifiedTableName).
			Set(t.nonKeyColumns...).
			Where(t.allKeyPredicates...)

		additionalVals := map[string]any{}

		for _, opt := range opts {
			builder = opt.applyToUpdateBuilder(builder)
			maps.Copy(additionalVals, opt.getMapData())
		}

		query = builder.QueryContext(ctx, t.Session).
			BindStructMap(instance, additionalVals)
	}

	defer query.Release()
	t.Logger.Debug("upsert by primary key", zap.Stringer("query", query))

	// The query timeout starts once we hold an execution slot, so queueing behind
	// other executions doesn't erode the budget for our own round-trips.
	if errGate := gate.enter(ctx); errGate != nil {
		return errGate
	}
	defer gate.exit()

	st := time.Now()
	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()
	query = query.WithContext(retryCtx)

	for {
		err := query.Exec()
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

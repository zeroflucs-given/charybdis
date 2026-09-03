package tables

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Insert inserts a single object. Unlike upsert it enforces the value does not exist. You can achieve
// the same effect with an Upsert if you use the WithNotExist option.
func (t *tableManagerImpl[T]) Insert(ctx context.Context, instance *T, opts ...InsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/Insert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.insertInternal(ctx, instance, true, nil, opts...)
	})
}

// InsertOrReplace inserts a single object or replaces if it already exists. This is effectively an upsert that
// works for tables with no non-key columns.
func (t *tableManagerImpl[T]) InsertOrReplace(ctx context.Context, instance *T, opts ...InsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/Insert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.insertInternal(ctx, instance, false, nil, opts...)
	})
}

// InsertBulk inserts many objects in parallel, up to a given number. If the concurrency limit is not set,
// then a default of DefaultBulkConcurrency is used. The limit applies to statement executions only;
// preparation work (hooks, query building, binding) runs in parallel across all instances.
func (t *tableManagerImpl[T]) InsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...InsertOption) error {
	if concurrency <= 0 {
		concurrency = DefaultBulkConcurrency
	}

	return doWithTracing(ctx, t.Tracer, t.Name+"/InsertBulk", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		gate := make(execGate, concurrency)
		grp, grpCtx := errgroup.WithContext(ctx)

		for _, v := range instances {
			item := v
			grp.Go(func() error {
				return t.insertInternal(grpCtx, item, true, gate, opts...)
			})
		}

		return grp.Wait()
	})
}

// insertInternal is a helper function that performs a single upsert
func (t *tableManagerImpl[T]) insertInternal(ctx context.Context, instance *T, enforceNotExists bool, gate execGate, opts ...InsertOption) error {
	// Pre-change hooks
	err := t.runPreHooks(ctx, instance)
	if err != nil {
		return err
	}

	isLWT := false
	if enforceNotExists {
		// We must not exist
		opts = append(opts, WithNotExists())
		isLWT = true
	}

	// Build our query
	query := qb.Insert(t.qualifiedTableName).Columns(t.allColumnNames...)

	for _, opt := range opts {
		query = opt.applyToInsertBuilder(query)
		isLWT = isLWT || opt.isPrecondition()
	}

	stmt, params := query.ToCql()

	var applied bool
	q := t.Session.ContextQuery(ctx, stmt, params).
		Consistency(t.writeConsistency).
		BindStruct(instance)

	t.Logger.Debug("insert", zap.Stringer("query", q))
	defer q.Release()

	// The query timeout starts once we hold an execution slot, so queueing behind
	// other executions doesn't erode the budget for our own round-trips.
	if errGate := gate.enter(ctx); errGate != nil {
		return errGate
	}
	defer gate.exit()

	st := time.Now()
	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()
	q = q.WithContext(retryCtx)

	for {
		if isLWT {
			applied, err = q.ExecCAS()
			if !applied {
				t.Logger.Debug("inserted no rows", zap.Stringer("query", q))
			}
		} else {
			err = q.Exec()
		}

		if err == nil {
			break
		}

		var wto *gocql.RequestErrWriteTimeout
		retryable := errors.As(err, &wto)
		if !retryable {
			t.Logger.Debug("failure not retryable", zap.Error(err))
			return err
		}

		t.Logger.Debug("insert retrying from early write timeout",
			zap.String("consistency", wto.Consistency.String()),
			zap.Int("received", wto.Received),
			zap.Int("blockFor", wto.BlockFor),
			zap.String("writeType", wto.WriteType),
			zap.Duration("set_timeout", t.queryTimeout),
			zap.Duration("execution_time_to_now", time.Since(st)),
		)
	}

	if isLWT && !applied {
		return ErrPreconditionFailed
	}

	// Post-change hooks
	errPost := t.runPostHooks(ctx, instance)
	if errPost != nil {
		return errPost
	}

	return nil
}

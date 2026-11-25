package tables

import (
	"context"
	"sync/atomic"

	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// Insert inserts a single object. Unlike upsert it enforces the value does not exist. You can achieve
// the same effect with an Upsert if you use the WithNotExist option.
func (t *tableManagerImpl[T]) Insert(ctx context.Context, instance *T, opts ...InsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name, "Insert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.insertInternal(ctx, instance, true, nil, opts...)
	})
}

// InsertOrReplace inserts a single object or replaces if it already exists. This is effectively an upsert that
// works for tables with no non-key columns.
func (t *tableManagerImpl[T]) InsertOrReplace(ctx context.Context, instance *T, opts ...InsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name, "Insert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.insertInternal(ctx, instance, false, nil, opts...)
	})
}

// InsertBulk inserts many objects in parallel, up to a given number. If the concurrency limit is not set,
// then a default of DefaultBulkConcurrency is used. The limit applies to statement executions only;
// preparation work (hooks, query building, binding) runs ahead on a bounded pool of workers.
func (t *tableManagerImpl[T]) InsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...InsertOption) error {
	if concurrency <= 0 {
		concurrency = DefaultBulkConcurrency
	}

	return doWithTracing(ctx, t.Tracer, t.Name, "InsertBulk", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
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
					if err := t.insertInternal(grpCtx, instances[i], true, gate, opts...); err != nil {
						return err
					}
				}
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
	var stmt string
	var params []string

	if len(opts) == 0 {
		if enforceNotExists {
			stmt, params = t.insertNotExistsStmt, t.insertNotExistsNames
			isLWT = true
		} else {
			stmt, params = t.insertStmt, t.insertNames
		}
	} else {
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

		stmt, params = query.ToCql()
	}

	var applied bool
	q := t.Session.ContextQuery(ctx, stmt, params).
		Consistency(t.writeConsistency)
	if len(opts) == 0 && t.codec != nil {
		q = q.Bind(t.codec.BindColumns(instance, params)...)
	} else {
		q = q.BindStruct(instance)
	}

	t.Logger.Debug("insert", zap.Stringer("query", q))
	defer q.Release()

	// The query timeout starts once we hold an execution slot, so queueing behind
	// other executions doesn't erode the budget for our own round-trips.
	if errGate := gate.enter(ctx); errGate != nil {
		return errGate
	}
	defer gate.exit()

	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()
	q = q.WithContext(retryCtx)

	err = retryWriteTimeouts(retryCtx, t.Logger, "insert", func() error {
		if !isLWT {
			return q.Exec()
		}
		var errExec error
		applied, errExec = q.ExecCAS()
		if !applied {
			t.Logger.Debug("inserted no rows", zap.Stringer("query", q))
		}
		return errExec
	})
	if err != nil {
		return err
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

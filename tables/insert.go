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

// Insert inserts a single object. Unlike upsert it enforces the value does not exist. You can achieve
// the same effect with an Upsert if you use the WithNotExist option.
func (t *tableManagerImpl[T]) Insert(ctx context.Context, instance *T, opts ...InsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/Insert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.insertInternal(ctx, instance, true, opts...)
	})
}

// InsertOrReplace inserts a single object or replaces if it already exists. This is effectively an upsert that
// works for tables with no non-key columns.
func (t *tableManagerImpl[T]) InsertOrReplace(ctx context.Context, instance *T, opts ...InsertOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/Insert", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.insertInternal(ctx, instance, false, opts...)
	})
}

// InsertBulk inserts many objects in parallel, up to a given number. If the concurrency limit is not set,
// then a default of DefaultBulkConcurrency is used.
func (t *tableManagerImpl[T]) InsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...InsertOption) error {
	if concurrency <= 0 {
		concurrency = DefaultBulkConcurrency
	}

	return doWithTracing(ctx, t.Tracer, t.Name+"/InsertBulk", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		grp, grpCtx := errgroup.WithContext(ctx)
		grp.SetLimit(concurrency)

		for _, v := range instances {
			item := v
			grp.Go(func() error {
				return t.insertInternal(grpCtx, item, true, opts...)
			})
		}

		return grp.Wait()
	})
}

// insertInternal is a helper function that performs a single upsert
func (t *tableManagerImpl[T]) insertInternal(ctx context.Context, instance *T, enforceNotExists bool, opts ...InsertOption) error {
	// Pre-change hooks
	err := t.runPreHooks(ctx, instance)
	if err != nil {
		return err
	}

	if enforceNotExists {
		// We must not exist
		opts = append(opts, WithNotExists())
	}

	// Build our query
	query := qb.Insert(t.qualifiedTableName).Columns(t.allColumnNames...)

	for _, opt := range opts {
		query = opt.applyToInsertBuilder(query)
	}

	st := time.Now()
	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()

	var applied bool
	stmt, params := query.ToCql()
	for {
		q := t.Session.ContextQuery(retryCtx, stmt, params).
			Consistency(t.writeConsistency).
			BindStruct(instance)

		applied, err = q.ExecCASRelease()

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

	if !applied {
		return ErrPreconditionFailed
	}

	// Post-change hooks
	errPost := t.runPostHooks(ctx, instance)
	if errPost != nil {
		return errPost
	}

	return nil
}

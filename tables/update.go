package tables

import (
	"context"
	"errors"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"
)

// Update updates an object. It will error if the object does not exist.
func (t *tableManagerImpl[T]) Update(ctx context.Context, instance *T, opts ...UpdateOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/Update", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.updateInternal(ctx, instance, opts...)
	})
}

// updateInternal is a helper function that performs a single update
func (t *tableManagerImpl[T]) updateInternal(ctx context.Context, instance *T, opts ...UpdateOption) error {
	// Pre-change hooks
	err := t.runPreHooks(ctx, instance)
	if err != nil {
		return err
	}

	// Build our query
	query := qb.Update(t.qualifiedTableName).
		Set(t.nonKeyColumns...).
		Where(t.allKeyPredicates...)

	additionalVals := map[string]any{}
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

	st := time.Now()
	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()

	var applied bool
	stmt, params := query.ToCql()
	for {
		applied, err = t.Session.
			ContextQuery(retryCtx, stmt, params).
			BindStructMap(instance, additionalVals).
			ExecCASRelease()

		if err == nil {
			break
		}

		var wto *gocql.RequestErrWriteTimeout
		retryable := errors.As(err, &wto)
		if !retryable {
			return err
		}

		t.Logger.Debug("update retrying from early write timeout",
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

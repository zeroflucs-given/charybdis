package tables

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"
)

// Delete removes an object by binding against the structure values. Practically, only the keys of the object need be set.
func (t *tableManagerImpl[T]) Delete(ctx context.Context, instance *T) error {
	if instance == nil {
		return nil // nothing to delete
	}

	return doWithTracing(ctx, t.Tracer, t.Name+"/DeleteByObject", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		// Pre-delete hooks
		if len(t.preDeleteHooks) > 0 {
			existing, err := t.GetByExample(ctx, instance)
			if err != nil {
				return fmt.Errorf("error fetching existing record for pre-delete hooks: %w", err)
			}
			errHooks := t.runPreDeleteHooks(ctx, existing)
			if errHooks != nil {
				return fmt.Errorf("error running pre-delete hooks: %w", errHooks)
			}
		}

		st := time.Now()
		retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
		defer cancel()

		q := t.Table.
			DeleteBuilder().
			Existing().
			QueryContext(retryCtx, t.Session).
			Consistency(t.writeConsistency).
			BindStruct(instance)

		queryString := q.String()
		t.Logger.Debug("delete using struct binding", zap.String("query", queryString))

		defer q.Release()

		var timeRemaining AsStringerFunc = func() string {
			return time.Since(st).String()
		}

		logger := t.Logger.
			With(
				zap.String("operation", "delete"),
				zap.String("query", queryString),
				zap.Duration("timeout", t.queryTimeout),
			).
			WithLazy(
				zap.Stringer("execution_time_to_now", timeRemaining),
			)

		return retryBeforeTimeout(logger, q, false)
	})
}

// DeleteByPrimaryKey removes a single row by primary key
func (t *tableManagerImpl[T]) DeleteByPrimaryKey(ctx context.Context, keys ...any) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/DeleteByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.deleteInternal(ctx, WithDeletePredicates(t.Table.PrimaryKeyCmp()[:len(keys)]...), WithDeletionBindings(keys...))
	})
}

// DeleteUsingOptions removes rows/columns specified with the supplied options
func (t *tableManagerImpl[T]) DeleteUsingOptions(ctx context.Context, opts ...DeleteOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/DeleteUsingOptions", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.deleteInternal(ctx, opts...)
	})
}

func (t *tableManagerImpl[T]) deleteInternal(ctx context.Context, opts ...DeleteOption) error {
	var cols []string
	var predicates []qb.Cmp
	var bindings []any
	var ifConditions []qb.Cmp
	var ifExists bool

	for _, opt := range opts {
		cols = append(cols, opt.columns()...)
		predicates = append(predicates, opt.predicates()...)
		bindings = append(bindings, opt.bindings()...)
		ifConditions = append(ifConditions, opt.ifConditions()...)
		ifExists = ifExists || opt.ifExists()
	}

	// Pre-delete hooks
	if len(t.preDeleteHooks) > 0 {
		existing, err := t.GetUsingOptions(ctx, WithPredicates(predicates...), WithBindings(bindings...))
		if err != nil {
			return fmt.Errorf("fetching existing record for pre-delete hooks: %w", err)
		}
		errHooks := t.runPreDeleteHooks(ctx, existing)
		if errHooks != nil {
			return fmt.Errorf("running pre-delete hooks: %w", errHooks)
		}
	}

	st := time.Now()
	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()

	isLWT := len(ifConditions) > 0 || ifExists
	builder := qb.Delete(t.qualifiedTableName).
		Columns(cols...).
		Where(predicates...)

	if len(ifConditions) > 0 {
		builder = builder.If(ifConditions...)
	} else if ifExists {
		builder = builder.Existing()
	}

	query := t.Session.
		Query(builder.ToCql()).
		WithContext(retryCtx).
		Consistency(t.writeConsistency)

	for _, opt := range opts {
		query = opt.applyToQuery(query)
	}
	query = query.Bind(bindings...)

	queryString := query.String()
	t.Logger.Debug("delete using options", zap.String("query", queryString))

	defer query.Release()

	var timeRemaining AsStringerFunc = func() string {
		return time.Since(st).String()
	}

	logger := t.Logger.
		With(
			zap.String("operation", "delete"),
			zap.String("query", queryString),
			zap.Duration("timeout", t.queryTimeout),
		).
		WithLazy(
			zap.Stringer("execution_time_to_now", timeRemaining),
		)

	return retryBeforeTimeout(logger, query, isLWT)
}

func retryBeforeTimeout[E Executable](logger *zap.Logger, query E, isLWT bool) error {
	for {
		var err error
		if isLWT {
			var applied bool
			applied, err = query.ExecCAS()
			if !applied {
				logger.Debug("no rows affected")
			}
		} else {
			err = query.Exec()
		}

		if err == nil {
			break
		}

		var wto *gocql.RequestErrWriteTimeout
		if !errors.As(err, &wto) {
			logger.Debug("failure not retryable", zap.Error(err))
			return err
		}

		logger.Info("retrying before timeout",
			zap.String("consistency", wto.Consistency.String()),
			zap.Int("received", wto.Received),
			zap.Int("blockFor", wto.BlockFor),
			zap.String("writeType", wto.WriteType),
		)
	}

	return nil
}

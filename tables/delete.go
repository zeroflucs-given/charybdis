package tables

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2/qb"
	"go.uber.org/zap"
)

// Delete removes an object by binding against the structure values. Technically only the
// keys of the object need be set.
func (t *tableManagerImpl[T]) Delete(ctx context.Context, instance *T) error {
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

		for {
			err := t.Table.
				DeleteQueryContext(retryCtx, t.Session).
				Consistency(t.writeConsistency).
				BindStruct(instance).
				Exec()

			if err == nil {
				break
			}

			var wto *gocql.RequestErrWriteTimeout
			retryable := errors.As(err, &wto)
			if !retryable {
				return err
			}

			t.Logger.Debug("delete retrying from early write timeout",
				zap.String("consistency", wto.Consistency.String()),
				zap.Int("received", wto.Received),
				zap.Int("blockFor", wto.BlockFor),
				zap.String("writeType", wto.WriteType),
				zap.Duration("set_timeout", t.queryTimeout),
				zap.Duration("execution_time_to_now", time.Since(st)),
			)
		}

		return nil
	})
}

// DeleteByPrimaryKey removes a single row by primary key
func (t *tableManagerImpl[T]) DeleteByPrimaryKey(ctx context.Context, keys ...any) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/DeleteByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		// Pre-delete hooks
		if len(t.preDeleteHooks) > 0 {
			existing, err := t.GetByPrimaryKey(ctx, keys...)
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

		for {
			err := t.Table.
				DeleteQueryContext(retryCtx, t.Session).
				Consistency(t.writeConsistency).
				Bind(keys...).
				Exec()

			if err == nil {
				break
			}

			var wto *gocql.RequestErrWriteTimeout
			retryable := errors.As(err, &wto)
			if !retryable {
				return err
			}

			t.Logger.Info("delete retrying from early write timeout",
				zap.String("consistency", wto.Consistency.String()),
				zap.Int("received", wto.Received),
				zap.Int("blockFor", wto.BlockFor),
				zap.String("writeType", wto.WriteType),
				zap.Duration("set_timeout", t.queryTimeout),
				zap.Duration("execution_time_to_now", time.Since(st)),
			)
		}

		return nil
	})
}

// DeleteUsingOptions removes rows/columns specified with the supplied options
func (t *tableManagerImpl[T]) DeleteUsingOptions(ctx context.Context, opts ...DeleteOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/DeleteByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		var cols []string
		var predicates []qb.Cmp
		var bindings []any

		for _, opt := range opts {
			cols = append(cols, opt.columns()...)
			predicates = append(predicates, opt.predicates()...)
			bindings = append(bindings, opt.bindings()...)
		}

		// Pre-delete hooks
		if len(t.preDeleteHooks) > 0 {
			existing, err := t.GetUsingOptions(ctx, WithPredicates(predicates...), WithBindings(bindings...))
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

		builder := qb.Delete(t.qualifiedTableName).Columns(cols...).Where(predicates...)

		query := t.Session.Query(builder.ToCql()).
			WithContext(retryCtx).
			Consistency(t.writeConsistency)

		for _, opt := range opts {
			query = opt.applyToQuery(query)
		}
		query.Bind(bindings...)

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

			t.Logger.Info("delete retrying from early write timeout",
				zap.String("consistency", wto.Consistency.String()),
				zap.Int("received", wto.Received),
				zap.Int("blockFor", wto.BlockFor),
				zap.String("writeType", wto.WriteType),
				zap.Duration("set_timeout", t.queryTimeout),
				zap.Duration("execution_time_to_now", time.Since(st)),
			)
		}

		return nil
	})
}

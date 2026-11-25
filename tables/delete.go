package tables

import (
	"context"
	"fmt"

	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"
)

// Delete removes an object by binding against the structure values. Practically, only the keys of the object need be set.
func (t *tableManagerImpl[T]) Delete(ctx context.Context, instance *T) error {
	if instance == nil {
		return nil // nothing to delete
	}

	return doWithTracing(ctx, t.Tracer, t.Name, "DeleteByObject", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
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

		retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
		defer cancel()

		q := t.Table.
			DeleteBuilder().
			Existing().
			QueryContext(retryCtx, t.Session).
			Consistency(t.writeConsistency).
			BindStruct(instance)

		t.Logger.Debug("delete using struct binding", zap.Stringer("query", q))

		defer q.Release()

		return retryWriteTimeouts(retryCtx, t.Logger, "delete", q.Exec)
	})
}

// DeleteByPrimaryKey removes a single row by primary key
func (t *tableManagerImpl[T]) DeleteByPrimaryKey(ctx context.Context, keys ...any) error {
	return doWithTracing(ctx, t.Tracer, t.Name, "DeleteByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.deleteInternal(ctx, WithDeletePredicates(t.Table.PrimaryKeyCmp()[:len(keys)]...), WithDeletionBindings(keys...))
	})
}

// DeleteUsingOptions removes rows/columns specified with the supplied options
func (t *tableManagerImpl[T]) DeleteUsingOptions(ctx context.Context, opts ...DeleteOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name, "DeleteUsingOptions", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.deleteInternal(ctx, opts...)
	})
}

// Truncate the table, leaving it with no rows
func (t *tableManagerImpl[T]) Truncate(ctx context.Context) error {
	return doWithTracing(ctx, t.Tracer, t.Name, "Truncate", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		query := t.Session.
			Query("TRUNCATE "+t.qualifiedTableName, nil).
			WithContext(ctx).
			Consistency(t.writeConsistency)
		defer query.Release()
		t.Logger.Debug("truncate", zap.String("operation", "truncate"), zap.Stringer("query", query))
		return query.Exec()
	})
}

func (t *tableManagerImpl[T]) deleteInternal(ctx context.Context, opts ...DeleteOption) error {
	var bindings []any
	var isLWT bool
	var predicates []qb.Cmp

	for _, opt := range opts {
		isLWT = isLWT || opt.isPrecondition()
		predicates = append(predicates, opt.conditions()...)
		bindings = append(bindings, opt.bindings()...)
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

	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()

	builder := qb.Delete(t.qualifiedTableName)

	for _, b := range opts {
		builder = b.applyToBuilder(builder)
	}

	query := t.Session.
		Query(builder.ToCql()).
		WithContext(retryCtx).
		Consistency(t.writeConsistency)

	for _, mut := range opts {
		query = mut.applyToQuery(query)
	}
	query = query.Bind(bindings...)

	t.Logger.Debug("delete using options", zap.Stringer("query", query))

	defer query.Release()

	return retryWriteTimeouts(retryCtx, t.Logger, "delete", func() error {
		if !isLWT {
			return query.Exec()
		}
		applied, errExec := query.ExecCAS()
		if !applied {
			t.Logger.Debug("no rows affected", zap.Stringer("query", query))
		}
		return errExec
	})
}

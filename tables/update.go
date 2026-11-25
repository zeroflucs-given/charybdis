package tables

import (
	"context"
	"maps"

	"github.com/scylladb/gocqlx/v3/qb"
)

// Update updates an object. It will error if the object does not exist.
func (t *tableManagerImpl[T]) Update(ctx context.Context, instance *T, opts ...UpdateOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name, "Update", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
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

	var stmt string
	var params []string
	var additionalVals map[string]any

	if len(opts) == 0 {
		stmt, params = t.updateStmt, t.updateNames
	} else {
		query := qb.Update(t.qualifiedTableName).
			Set(t.nonKeyColumns...).
			Where(t.allKeyPredicates...)

		additionalVals = map[string]any{}
		havePreconditions := false

		for _, opt := range opts {
			if opt.isPrecondition() {
				havePreconditions = true
			}
			query = opt.applyToUpdateBuilder(query)
			maps.Copy(additionalVals, opt.getMapData())
		}

		// If we have no other preconditions, add an IF EXISTS check
		if !havePreconditions {
			query = query.
				Existing()
		}

		stmt, params = query.ToCql()
	}

	retryCtx, cancel := context.WithTimeout(ctx, t.queryTimeout)
	defer cancel()

	var applied bool
	err = retryWriteTimeouts(retryCtx, t.Logger, "update", func() error {
		query := t.Session.ContextQuery(retryCtx, stmt, params)
		if len(opts) == 0 && t.codec != nil {
			query = query.Bind(t.codec.BindColumns(instance, params)...)
		} else {
			query = query.BindStructMap(instance, additionalVals)
		}
		var errExec error
		applied, errExec = query.ExecCASRelease()
		return errExec
	})
	if err != nil {
		return err
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

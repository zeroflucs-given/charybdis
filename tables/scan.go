package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v3"
)

// Scan performs an interactive scan of the data in the table.
func (t *baseManagerImpl[T]) Scan(ctx context.Context, fn PageHandlerFn[T], opts ...QueryOption) error {
	return t.pageQueryInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
		stmt, params := t.basicQueryBuilder(opts...).ToCql()

		query := sess.ContextQuery(ctx, stmt, params)

		// t.Logger.Debug("scan statement", zap.String("query", query.String()))

		return query
	}, fn, true, opts...)
}

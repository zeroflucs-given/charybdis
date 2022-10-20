package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
)

// Scan performs an iteractive scan of the data in the table.
func (t *baseManagerImpl[T]) Scan(ctx context.Context, fn PageHandlerFn[T], opts ...QueryOption) error {
	return t.pageQueryInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
		stmt, params := t.Table.SelectAll()
		query := sess.ContextQuery(ctx, stmt, params)
		return query
	}, fn, opts...)
}

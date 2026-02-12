package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

// Count the number of records in the table.
func (t *baseManagerImpl[T]) Count(ctx context.Context) (int64, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/Count", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (int64, error) {
		return t.countInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
			stmt, params := qb.
				Select(t.Table.Name()).
				Columns("COUNT(1)").
				ToCql()

			return sess.ContextQuery(ctx, stmt, params).
				Consistency(t.readConsistency)
		})
	})
}

// CountByPartitionKey gets the number of records in the partition.
func (t *baseManagerImpl[T]) CountByPartitionKey(ctx context.Context, partitionKeys ...any) (int64, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/CountByPartitionKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (int64, error) {
		return t.countInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
			stmt, params := qb.
				Select(t.Table.Name()).
				Columns("COUNT(1)").
				Where(t.partitionKeyPredicates...).
				ToCql()

			return sess.ContextQuery(ctx, stmt, params).
				Consistency(t.readConsistency).
				Bind(partitionKeys...)
		})
	})
}

// CountByCustomQuery gets the number of records in a custom query.
func (t *baseManagerImpl[T]) CountByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn) (int64, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/CountByCustomQuery", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (int64, error) {
		return t.countInternal(ctx, queryBuilder)
	})
}

// countInternal performs the counting queries
func (t *baseManagerImpl[T]) countInternal(ctx context.Context, queryBuilder QueryBuilderFn) (int64, error) {
	var count int64
	query := queryBuilder(ctx, t.Session)
	return count, query.Scan(&count)
}

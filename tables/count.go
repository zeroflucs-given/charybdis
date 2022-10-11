package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// Count the number of records in the table.
func (t *tableManagerImpl[T]) Count(ctx context.Context) (int64, error) {
	return returnWithTracing(ctx, t.Tracer, t.Spec.Name+"/Count", t.TraceAttributes, func(ctx context.Context) (int64, error) {
		return t.countInternal(ctx, func(ctx context.Context) *gocqlx.Queryx {
			stmt, params := qb.
				Select(t.Table.Name()).
				Columns("COUNT(1)").
				ToCql()

			return t.Session.ContextQuery(ctx, stmt, params).
				Consistency(t.readConsistency)
		})
	})
}

// CountByPartitionKey gets the number of records in the partition.
func (t *tableManagerImpl[T]) CountByPartitionKey(ctx context.Context, partitionKeys ...interface{}) (int64, error) {
	return returnWithTracing(ctx, t.Tracer, t.Spec.Name+"/CountByPartitionKey", t.TraceAttributes, func(ctx context.Context) (int64, error) {
		return t.countInternal(ctx, func(ctx context.Context) *gocqlx.Queryx {
			stmt, params := qb.
				Select(t.Table.Name()).
				Columns("COUNT(1)").
				Where(t.partitionKeyPredicates...).
				ToCql()

			return t.Session.ContextQuery(ctx, stmt, params).
				Consistency(t.readConsistency).
				Bind(partitionKeys...)
		})
	})
}

// CountByCustomQuery gets the number of records in a custom query.
func (t *tableManagerImpl[T]) CountByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn) (int64, error) {
	return returnWithTracing(ctx, t.Tracer, t.Spec.Name+"/CountByCustomQuery", t.TraceAttributes, func(ctx context.Context) (int64, error) {
		return t.countInternal(ctx, queryBuilder)
	})
}

// countInternal performs the counting queries
func (t *tableManagerImpl[T]) countInternal(ctx context.Context, queryBuilder QueryBuilderFn) (int64, error) {
	var count int64
	query := queryBuilder(ctx)
	return count, query.Scan(&count)
}

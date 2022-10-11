package tables

import (
	"context"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// GetByPartitionKey gets the first record from a partition. If there are multiple records, the
// behaviour is to return the first record by clustering order.
func (t *tableManagerImpl[T]) GetByPartitionKey(ctx context.Context, partitionKeys ...interface{}) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Spec.Name+"/GetByPartitionKey", t.TraceAttributes, func(ctx context.Context) (*T, error) {
		stmt, params := qb.
			Select(t.Table.Name()).Columns(t.allColumnNames...).
			Where(t.partitionKeyPredicates...).
			ToCql()

		var target T
		errQuery := t.Session.ContextQuery(ctx, stmt, params).
			Consistency(t.readConsistency).
			Bind(partitionKeys...).
			Get(&target)
		return &target, errQuery
	})
}

// GetByPrimaryKey gets a record by primary key, including both partitioning and any clustering keys
func (t *tableManagerImpl[T]) GetByPrimaryKey(ctx context.Context, primaryKeys ...interface{}) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Spec.Name+"/GetByPrimaryKey", t.TraceAttributes, func(ctx context.Context) (*T, error) {
		stmt, params := qb.
			Select(t.Table.Name()).Columns(t.allColumnNames...).
			Where(t.allKeyPredicates...).
			ToCql()

		var target T
		errQuery := t.Session.ContextQuery(ctx, stmt, params).
			Consistency(t.readConsistency).
			Bind(primaryKeys...).
			Get(&target)
		return &target, errQuery
	})
}

// GetByIndexedColumn gets the first record matching an index
func (t *tableManagerImpl[T]) GetByIndexedColumn(ctx context.Context, columnName string, value interface{}, opts ...QueryOption) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Spec.Name+"/GetByIndexedColumn", t.TraceAttributes, func(ctx context.Context) (*T, error) {
		var target T

		stmt, params := qb.
			Select(t.Table.Name()).
			Columns(t.TableMetadata.Columns...).
			Where(qb.Eq(columnName)).
			ToCql()

		query := t.Session.ContextQuery(ctx, stmt, params).
			Consistency(t.readConsistency).
			Bind(value)

		for _, opt := range opts {
			query = opt.applyToQuery(query)
		}

		errQuery := query.
			Bind(value).
			Get(&target)

		if errQuery == gocql.ErrNotFound {
			return nil, nil
		} else if errQuery != nil {
			return nil, errQuery
		}

		return &target, nil
	})
}

// SelectByCustomQuery gets all records by a custom query in a paged fashion
func (t *tableManagerImpl[T]) SelectByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn, pagingFn PageHandlerFn[T], opts ...QueryOption) error {
	return doWithTracing(ctx, t.Tracer, t.Spec.Name+"/SelectByCustomQuery", t.TraceAttributes, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, queryBuilder, pagingFn, opts...)
	})
}

// SelectByIndexedColumn selects all records by an indexed column
func (t *tableManagerImpl[T]) SelectByIndexedColumn(ctx context.Context, fn PageHandlerFn[T], columnName string, columnValue interface{}, opts ...QueryOption) error {
	return doWithTracing(ctx, t.Tracer, t.Spec.Name+"/SelectByIndexedColumn", t.TraceAttributes, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, func(ctx context.Context) *gocqlx.Queryx {
			stmt, params := qb.
				Select(t.Table.Name()).
				Columns(t.TableMetadata.Columns...).
				Where(qb.Eq(columnName)).
				ToCql()

			return t.Session.ContextQuery(ctx, stmt, params).Bind(columnValue)
		}, fn, opts...)
	})
}

// SelectByPartitionKey gets all records from a partition
func (t *tableManagerImpl[T]) SelectByPartitionKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, partitionKeys ...interface{}) error {
	return doWithTracing(ctx, t.Tracer, t.Spec.Name+"/SelectByPartitionKey", t.TraceAttributes, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, func(ctx context.Context) *gocqlx.Queryx {
			return t.Table.
				SelectQueryContext(ctx, t.Session, t.allColumnNames...).Bind(partitionKeys...)
		}, fn, opts...)
	})
}

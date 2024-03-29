package tables

import (
	"context"
	"errors"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// GetByPartitionKey gets the first record from a partition. If there are multiple records, the
// behaviour is to return the first record by clustering order.
func (t *baseManagerImpl[T]) GetByPartitionKey(ctx context.Context, partitionKeys ...any) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetByPartitionKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
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
func (t *baseManagerImpl[T]) GetByPrimaryKey(ctx context.Context, primaryKeys ...any) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
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

// GetByExample gets a single record, binding by example object with the key fields all set
func (t *baseManagerImpl[T]) GetByExample(ctx context.Context, example *T) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetByExample", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
		stmt, params := qb.
			Select(t.Table.Name()).Columns(t.allColumnNames...).
			Where(t.allKeyPredicates...).
			ToCql()

		var target T
		errQuery := t.Session.ContextQuery(ctx, stmt, params).
			Consistency(t.readConsistency).
			BindStruct(example).
			Get(&target)
		return &target, errQuery
	})
}

// GetByIndexedColumn gets the first record matching an index
func (t *baseManagerImpl[T]) GetByIndexedColumn(ctx context.Context, columnName string, value any, opts ...QueryOption) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetByIndexedColumn", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
		var target T

		builder := qb.
			Select(t.Table.Name()).
			Columns(t.TableMetadata.Columns...).
			Where(qb.Eq(columnName))

		for _, opt := range opts {
			builder = opt.applyToBuilder(builder)
		}
		stmt, params := builder.ToCql()

		query := t.Session.ContextQuery(ctx, stmt, params).
			Consistency(t.readConsistency).
			Bind(value)

		for _, opt := range opts {
			query = opt.applyToQuery(query)
		}

		errQuery := query.
			Bind(value).
			Get(&target)

		if errors.Is(errQuery, gocql.ErrNotFound) {
			return nil, nil
		} else if errQuery != nil {
			return nil, errQuery
		}

		return &target, nil
	})
}

// SelectByCustomQuery gets all records by a custom query in a paged fashion
func (t *baseManagerImpl[T]) SelectByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn, pagingFn PageHandlerFn[T], opts ...QueryOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/SelectByCustomQuery", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, queryBuilder, pagingFn, opts...)
	})
}

// SelectByIndexedColumn selects all records by an indexed column
func (t *baseManagerImpl[T]) SelectByIndexedColumn(ctx context.Context, fn PageHandlerFn[T], columnName string, columnValue any, opts ...QueryOption) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/SelectByIndexedColumn", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
			builder := qb.
				Select(t.Table.Name()).
				Columns(t.TableMetadata.Columns...).
				Where(qb.Eq(columnName))

			for _, opt := range opts {
				opt.applyToBuilder(builder)
			}
			stmt, params := builder.ToCql()

			return sess.ContextQuery(ctx, stmt, params).Bind(columnValue)
		}, fn, opts...)
	})
}

// SelectByPartitionKey gets all records from a partition
func (t *baseManagerImpl[T]) SelectByPartitionKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, partitionKeys ...any) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/SelectByPartitionKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
			builder := t.Table.SelectBuilder(t.allColumnNames...)
			for _, opt := range opts {
				opt.applyToBuilder(builder)
			}
			return sess.Query(builder.ToCql()).WithContext(ctx).Bind(partitionKeys...)
		}, fn, opts...)
	})
}

// SelectByPrimaryKey gets all records by primary key, including both partitioning and zero or more clustering keys
func (t *baseManagerImpl[T]) SelectByPrimaryKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, primaryKeys ...any) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/SelectByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
			// trim predicates list to match length of primary keys entered in case not all clustering keys have been specified
			predicates := t.allKeyPredicates[:len(primaryKeys)]

			builder := qb.
				Select(t.Table.Name()).Columns(t.allColumnNames...).
				Where(predicates...)

			for _, opt := range opts {
				opt.applyToBuilder(builder)
			}

			stmt, params := builder.ToCql()

			return t.Session.ContextQuery(ctx, stmt, params).Bind(primaryKeys...)
		}, fn, opts...)
	})
}

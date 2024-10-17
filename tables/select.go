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
		var target T
		stmt, params := t.basicQueryBuilder().Where(t.partitionKeyPredicates...).ToCql()
		errQuery := t.basicQueryMutator(ctx, stmt, params).Bind(partitionKeys...).Get(&target)
		return &target, errQuery
	})
}

// GetByPrimaryKey gets a record by primary key, including both partitioning and any clustering keys
func (t *baseManagerImpl[T]) GetByPrimaryKey(ctx context.Context, primaryKeys ...any) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
		var target T
		stmt, params := t.basicQueryBuilder().Where(t.allKeyPredicates...).ToCql()
		errQuery := t.basicQueryMutator(ctx, stmt, params).Bind(primaryKeys...).Get(&target)
		return &target, errQuery
	})
}

// GetUsingOptions provides a method to fetch rows using QueryOptions to determine keys search & columns returned, etc
func (t *baseManagerImpl[T]) GetUsingOptions(ctx context.Context, opts ...QueryOption) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetUsingOptions", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
		var target T
		stmt, params := t.basicQueryBuilder(opts...).ToCql()
		errQuery := t.basicQueryMutator(ctx, stmt, params, opts...).Get(&target)
		return &target, errQuery
	})
}

// GetByExample gets a single record, binding by example object with the key fields all set
func (t *baseManagerImpl[T]) GetByExample(ctx context.Context, example *T) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetByExample", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
		var target T
		stmt, params := t.basicQueryBuilder().Where(t.allKeyPredicates...).ToCql()
		errQuery := t.basicQueryMutator(ctx, stmt, params).BindStruct(example).Get(&target)
		return &target, errQuery
	})
}

// GetByIndexedColumn gets the first record matching an index
func (t *baseManagerImpl[T]) GetByIndexedColumn(ctx context.Context, columnName string, value any, opts ...QueryOption) (*T, error) {
	return returnWithTracing(ctx, t.Tracer, t.Name+"/GetByIndexedColumn", t.TraceAttributes, t.DoTracing, func(ctx context.Context) (*T, error) {
		var target T
		stmt, params := t.basicQueryBuilder(opts...).Where(qb.Eq(columnName)).ToCql()

		errQuery := t.basicQueryMutator(ctx, stmt, params, opts...).Bind(value).Get(&target)
		if errors.Is(errQuery, gocql.ErrNotFound) {
			return nil, nil
		}

		if errQuery != nil {
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
			stmt, params := t.basicQueryBuilder(opts...).Where(qb.Eq(columnName)).ToCql()
			query := t.sessionQueryMutator(ctx, sess, stmt, params).Bind(columnValue)
			return query
		}, fn, opts...)
	})
}

// SelectByPartitionKey gets all records from a partition
func (t *baseManagerImpl[T]) SelectByPartitionKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, partitionKeys ...any) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/SelectByPartitionKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
			stmt, params := t.basicQueryBuilder(opts...).Where(t.partitionKeyPredicates...).ToCql()
			return t.sessionQueryMutator(ctx, sess, stmt, params).Bind(partitionKeys...)
		}, fn, opts...)
	})
}

// SelectByPrimaryKey gets all records by primary key, including both partitioning and zero or more clustering keys
func (t *baseManagerImpl[T]) SelectByPrimaryKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, primaryKeys ...any) error {
	return doWithTracing(ctx, t.Tracer, t.Name+"/SelectByPrimaryKey", t.TraceAttributes, t.DoTracing, func(ctx context.Context) error {
		return t.pageQueryInternal(ctx, func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx {
			// trim predicates list to match length of primary keys entered in case not all clustering keys have been specified
			predicates := t.allKeyPredicates[:len(primaryKeys)]
			stmt, params := t.basicQueryBuilder(opts...).Where(predicates...).ToCql()
			return t.sessionQueryMutator(ctx, sess, stmt, params).Bind(primaryKeys...)
		}, fn, opts...)
	})
}

// Construct a (partial) query builder using the given options
func (t *baseManagerImpl[T]) basicQueryBuilder(opts ...QueryOption) *qb.SelectBuilder {
	builder := qb.Select(t.Table.Name())

	var cols []string
	for _, opt := range opts {
		builder = opt.applyToBuilder(builder)
		cols = append(cols, opt.columns()...)
	}

	if len(cols) == 0 {
		cols = t.TableMetadata.Columns
	}
	builder = builder.Columns(cols...)

	return builder
}

// Construct a (partial) query using the given options
func (t *baseManagerImpl[T]) basicQueryMutator(ctx context.Context, stmt string, names []string, opts ...QueryOption) *gocqlx.Queryx {
	query := t.Session.ContextQuery(ctx, stmt, names).Consistency(t.readConsistency)

	var bindings []any
	for _, opt := range opts {
		query = opt.applyToQuery(query)
		bindings = append(bindings, opt.bindings())
	}
	query.Bind(bindings...)
	return query
}

// Construct a (partial) query using the given options & session
func (t *baseManagerImpl[T]) sessionQueryMutator(ctx context.Context, sess gocqlx.Session, stmt string, names []string, opts ...QueryOption) *gocqlx.Queryx {
	query := sess.ContextQuery(ctx, stmt, names)
	var bindings []any
	for _, opt := range opts {
		query = opt.applyToQuery(query)
		bindings = append(bindings, opt.bindings())
	}
	query.Bind(bindings...)
	return query
}

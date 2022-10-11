package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
)

// TableManager is an object that provides an abstraction over a table in ScyllaDB
type TableManager[T any] interface {
	// Count the number of records in the table.
	Count(ctx context.Context) (int64, error)

	// CountByPartitionKey gets the number of records in the partition.
	CountByPartitionKey(ctx context.Context, partitionKeys ...interface{}) (int64, error)

	// CountByCustomQuery gets the number of records in a custom query.
	CountByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn) (int64, error)

	// Delete removes an object. Technically only the object keys need be present.
	Delete(ctx context.Context, instance *T) error

	// DeleteByPrimaryKey removes a single row by its primary key values. Keys must be specified in order.
	DeleteByPrimaryKey(ctx context.Context, keys ...interface{}) error

	// GetByPartitionKey gets the first record from a partition. If there are multiple records, the
	// behaviour is to return the first record by clustering order. Equivalent to GetByPrimaryKey
	// if no clustering key is set
	GetByPartitionKey(ctx context.Context, keys ...interface{}) (*T, error)

	// GetByPrimaryKey gets by the full primary key (partitioning and clustering keys)
	GetByPrimaryKey(ctx context.Context, primaryKeys ...interface{}) (*T, error)

	// GetByIndexedColumn gets the first record matching an index
	GetByIndexedColumn(ctx context.Context, columnName string, value interface{}, opts ...QueryOption) (*T, error)

	// Insert a single record
	Insert(ctx context.Context, instance *T, options ...InsertOption) error

	// UpsertBulk upserts a many objects in parallel, up to a given number. If the concurrency limit is not set,
	// then a default of DefaultBulkConcurrency is used.
	InsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...InsertOption) error

	// Scan performs a paged scan of the table, processing each batch of records. If the ScanFn returns true,
	// the scan will continue advancing until no more records are returned.
	Scan(ctx context.Context, fn PageHandlerFn[T], opts ...QueryOption) error

	// SelectByCustomQuery gets all records by a custom query in a paged fashion
	SelectByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn, pagingFn PageHandlerFn[T], opts ...QueryOption) error

	// SelectByPartitionKey gets all records from a partition
	SelectByPartitionKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, partitionKeys ...interface{}) error

	// SelectByIndexedColumn gets all records matching an indexed column
	SelectByIndexedColumn(ctx context.Context, fn PageHandlerFn[T], columnName string, columnValue interface{}, opts ...QueryOption) error

	// Update an object. Will error if the object does not exist.
	Update(ctx context.Context, instance *T, opts ...UpdateOption) error

	// Upsert overwrites or inserts an object.
	Upsert(ctx context.Context, instance *T, opts ...UpdateOption) error
}

// InsertOption is an interface that describes options that can mutate an insert
type InsertOption interface {
	applyToInsertBuilder(builder *qb.InsertBuilder) *qb.InsertBuilder
}

// QueryOption is an interface that describes options that can mutate a scan.
type QueryOption interface {
	applyToQuery(query *gocqlx.Queryx) *gocqlx.Queryx
}

// UpdateOption is an interface that describes options that can mutate an update
type UpdateOption interface {
	// applyToUpdateBuilder mutates our query
	applyToUpdateBuilder(builder *qb.UpdateBuilder) *qb.UpdateBuilder

	// getMapData gets any additional key-values that the predicate requires
	getMapData() map[string]interface{}
}

// UpsertOption is an option that can be used for inserts or update
type UpsertOption interface {
	InsertOption
	UpdateOption
}
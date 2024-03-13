package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/zeroflucs-given/charybdis/metadata"
)

// TableManager is an object that provides an abstraction over a table in ScyllaDB
type TableManager[T any] interface {
	// Count the number of records in the table.
	Count(ctx context.Context) (int64, error)

	// CountByPartitionKey gets the number of records in the partition.
	CountByPartitionKey(ctx context.Context, partitionKeys ...any) (int64, error)

	// CountByCustomQuery gets the number of records in a custom query.
	CountByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn) (int64, error)

	// Delete removes an object. Technically only the object keys need be present.
	Delete(ctx context.Context, instance *T) error

	// DeleteByPrimaryKey removes a single row by its primary key values. Keys must be specified in order.
	DeleteByPrimaryKey(ctx context.Context, keys ...any) error

	// GetByPartitionKey gets the first record from a partition. If there are multiple records, the
	// behaviour is to return the first record by clustering order. Equivalent to GetByPrimaryKey
	// if no clustering key is set
	GetByPartitionKey(ctx context.Context, keys ...any) (*T, error)

	// GetByPrimaryKey gets by the full primary key (partitioning and clustering keys)
	GetByPrimaryKey(ctx context.Context, primaryKeys ...any) (*T, error)

	// GetByIndexedColumn gets the first record matching an index
	GetByIndexedColumn(ctx context.Context, columnName string, value any, opts ...QueryOption) (*T, error)

	// GetTableSpec gets the table specification for this table-manager
	GetTableSpec() *metadata.TableSpecification

	// Insert a single record
	Insert(ctx context.Context, instance *T, options ...InsertOption) error

	// InsertOrReplace inserts a single record if there is no existing record.
	InsertOrReplace(ctx context.Context, instance *T, options ...InsertOption) error

	// InsertBulk inserts many objects in parallel, up to a given number. If the concurrency limit is not set,
	// then a default of DefaultBulkConcurrency is used.
	InsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...InsertOption) error

	// Scan performs a paged scan of the table, processing each batch of records. If the ScanFn returns true,
	// the scan will continue advancing until no more records are returned.
	Scan(ctx context.Context, fn PageHandlerFn[T], opts ...QueryOption) error

	// SelectByCustomQuery gets all records by a custom query in a paged fashion
	SelectByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn, pagingFn PageHandlerFn[T], opts ...QueryOption) error

	// SelectByPartitionKey gets all records from a partition
	SelectByPartitionKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, partitionKeys ...any) error

	// SelectByPrimaryKey gets all records by partition key and any clustering keys provided
	SelectByPrimaryKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, primaryKeys ...any) error

	// SelectByIndexedColumn gets all records matching an indexed column
	SelectByIndexedColumn(ctx context.Context, fn PageHandlerFn[T], columnName string, columnValue any, opts ...QueryOption) error

	// Update an object. Will error if the object does not exist.
	Update(ctx context.Context, instance *T, opts ...UpdateOption) error

	// Upsert overwrites or inserts an object.
	Upsert(ctx context.Context, instance *T, opts ...UpdateOption) error

	// UpsertBulk upserts many objects in parallel, up to a given number. If the concurrency limit is not set,
	// then a default of DefaultBulkConcurrency is used.
	UpsertBulk(ctx context.Context, instances []*T, concurrency int, opts ...UpdateOption) error

	// AddPreChangeHook adds a pre-change hook. These hooks do not fire for deletes.
	AddPreChangeHook(hook ChangeHook[T])

	// AddPostChangeHook adds a post-change hook. Note that post-change hooks that fail
	// will leave the base tables updated. These hooks do not fire for deletes.
	AddPostChangeHook(hook ChangeHook[T])

	// AddPreDeleteHook adds a pre-delete hook. This will force an additional cost, in
	// that we must retrieve the full record first before.
	AddPreDeleteHook(hook ChangeHook[T])
}

// ViewManager is an object that provides an abstraction over a view in ScyllaDB
type ViewManager[T any] interface {
	// CountByPartitionKey gets the number of records in the partition.
	CountByPartitionKey(ctx context.Context, partitionKeys ...any) (int64, error)

	// CountByCustomQuery gets the number of records in a custom query.
	CountByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn) (int64, error)

	// GetByPartitionKey gets the first record from a partition. If there are multiple records, the
	// behaviour is to return the first record by clustering order. Equivalent to GetByPrimaryKey
	// if no clustering key is set
	GetByPartitionKey(ctx context.Context, keys ...any) (*T, error)

	// GetByPrimaryKey gets by the full primary key (partitioning and clustering keys)
	GetByPrimaryKey(ctx context.Context, primaryKeys ...any) (*T, error)

	// GetByIndexedColumn gets the first record matching an index
	GetByIndexedColumn(ctx context.Context, columnName string, value any, opts ...QueryOption) (*T, error)

	// Scan performs a paged scan of the table, processing each batch of records. If the ScanFn returns true,
	// the scan will continue advancing until no more records are returned.
	Scan(ctx context.Context, fn PageHandlerFn[T], opts ...QueryOption) error

	// SelectByCustomQuery gets all records by a custom query in a paged fashion
	SelectByCustomQuery(ctx context.Context, queryBuilder QueryBuilderFn, pagingFn PageHandlerFn[T], opts ...QueryOption) error

	// SelectByPartitionKey gets all records from a partition
	SelectByPartitionKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, partitionKeys ...any) error

	// SelectByPrimaryKey gets all records from a partition
	SelectByPrimaryKey(ctx context.Context, fn PageHandlerFn[T], opts []QueryOption, primaryKeys ...any) error

	// SelectByIndexedColumn gets all records matching an indexed column
	SelectByIndexedColumn(ctx context.Context, fn PageHandlerFn[T], columnName string, columnValue any, opts ...QueryOption) error
}

// InsertOption is an interface that describes options that can mutate an insert
type InsertOption interface {
	applyToInsertBuilder(builder *qb.InsertBuilder) *qb.InsertBuilder

	// isPrecondition indicates if this option applies a precondition to the query
	isPrecondition() bool
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
	getMapData() map[string]any

	// isPrecondition indicates if this option applies a precondition to the query
	isPrecondition() bool
}

// UpsertOption is an option that can be used for inserts or update
type UpsertOption interface {
	InsertOption
	UpdateOption
}

// ManagerOption defines an option for the table manager
type ManagerOption interface {
	mutateParameters(ctx context.Context, params *tableManagerParameters) error
	onStart(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error
	insertOptions() []InsertOption
	updateOptions() []UpdateOption
	upsertOptions() []UpsertOption
	beforeChange(ctx context.Context, rec any) error
	afterChange(ctx context.Context, rec any) error
}

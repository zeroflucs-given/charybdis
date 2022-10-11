package tables

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs/generics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// NewTableManager creates a table-manager instance
func NewTableManager[T any](ctx context.Context, options ...TableManagerOption) (TableManager[T], error) {
	// Apply parameters
	params := tableManagerParameters{}
	params.ensureDefaults()
	for _, opt := range options {
		err := opt.mutateParameters(ctx, &params)
		if err != nil {
			return nil, fmt.Errorf("error applying table manager option: %w", err)
		}
	}

	// Execute hooks
	for _, opt := range options {
		err := opt.onStart(ctx, params.Keyspace, params.TableSpec)
		if err != nil {
			return nil, fmt.Errorf("error running table manager start hooks: %w", err)
		}
	}

	// Validate table spec
	errTable := params.TableSpec.Validate()
	if errTable != nil {
		return nil, fmt.Errorf("error validating table spec: %w", errTable)
	}

	// Create our session
	wrappedSession, err := gocqlx.WrapSession(params.SessionFactory(params.Keyspace))
	if err != nil {
		return nil, fmt.Errorf("error wrapping session: %w", err)
	}

	table := params.TableSpec.ToCQLX()

	return &tableManagerImpl[T]{
		// Base objects
		Logger: params.Logger.With(
			zap.String("keyspace", params.Keyspace),
			zap.String("table", params.TableSpec.Name)),
		Tracer: otel.Tracer(TracingModuleName),

		// Metadata
		Session:       wrappedSession,
		Spec:          params.TableSpec,
		Table:         table,
		TableMetadata: table.Metadata(),

		// Helper data
		readConsistency:    params.ReadConsistency,
		writeConsistency:   params.WriteConsistency,
		qualifiedTableName: params.Keyspace + "." + params.TableSpec.Name,
		allColumnNames:     table.Metadata().Columns,
		nonKeyColumns: generics.Map(generics.Filter(params.TableSpec.Columns, func(i int, c *metadata.ColumnSpecification) bool {
			return !(c.IsPartitioningKey || c.IsClusteringKey)
		}), func(i int, c *metadata.ColumnSpecification) string {
			return c.Name
		}),
		partitionKeyPredicates: generics.Map(params.TableSpec.Partitioning, func(i int, c *metadata.PartitioningColumn) qb.Cmp {
			return qb.Eq(c.Column.Name)
		}),
		allKeyPredicates: generics.Map(generics.Filter(params.TableSpec.Columns, func(i int, c *metadata.ColumnSpecification) bool {
			return c.IsPartitioningKey || c.IsClusteringKey
		}), func(i int, c *metadata.ColumnSpecification) qb.Cmp {
			return qb.Eq(c.Name)
		}),
	}, nil
}

// tableManagerImpl is our underyling table manager implementation type. We make it private here
// to prevent embedding directly.
type tableManagerImpl[T any] struct {
	Session         gocqlx.Session               // Session
	Spec            *metadata.TableSpecification // Table spec
	Logger          *zap.Logger                  // Logger
	Tracer          trace.Tracer                 // OpenTelemetry tracer
	TraceAttributes []attribute.KeyValue         // Common trace attributes
	Table           *table.Table                 // Table helper
	TableMetadata   table.Metadata               // Table metadata

	// Helper data
	readConsistency        gocql.Consistency // Read consistency
	writeConsistency       gocql.Consistency // Write consistency
	qualifiedTableName     string            // Qualified table-name
	allColumnNames         []string          // Set of all column names
	nonKeyColumns          []string          // Non-key column names
	partitionKeyPredicates []qb.Cmp          // Partition key predicates
	allKeyPredicates       []qb.Cmp          // All key predicates, including partition key, in order
}

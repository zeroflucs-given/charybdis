package tables

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/generics"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// NewTableManager creates a table-manager instance
func NewTableManager[T any](ctx context.Context, options ...ManagerOption) (TableManager[T], error) {
	// Apply parameters
	params := tableManagerParameters{}
	params.ensureDefaults()
	for _, opt := range options {
		err := opt.mutateParameters(ctx, &params)
		if err != nil {
			return nil, fmt.Errorf("error applying table manager option: %w", err)
		}
	}

	var extraOps []metadata.DDLOperation

	if params.TTL.Seconds() != 0 && params.TableSpec != nil {
		extraOps = append(extraOps, metadata.DDLOperation{
			Description:  "Add default TTL if non 0",
			Command:      fmt.Sprintf("ALTER TABLE %s.%s WITH default_time_to_live = %v;", params.Keyspace, params.TableSpec.Name, int64(params.TTL.Seconds())),
			IgnoreErrors: []string{},
		})
	}

	// Execute hooks
	for _, opt := range options {
		err := opt.onStart(ctx, params.Keyspace, params.TableSpec, nil, extraOps...)
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
		baseManagerImpl: baseManagerImpl[T]{
			// Base objects
			Logger: params.Logger.With(
				zap.String("keyspace", params.Keyspace),
				zap.String("table", params.TableSpec.Name)),
			Tracer:    otel.Tracer(TracingModuleName),
			DoTracing: params.DoTracing,

			// Metadata
			Name:          params.TableSpec.Name,
			Session:       wrappedSession,
			Table:         table,
			TableMetadata: table.Metadata(),

			// Helper data
			readConsistency:    params.ReadConsistency,
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
		},

		tableSpec:        params.TableSpec,
		writeConsistency: params.WriteConsistency,
	}, nil
}

// tableManagerImpl is our underlying table manager implementation type. We make it private here
// to prevent embedding directly.
type tableManagerImpl[T any] struct {
	baseManagerImpl[T]

	// Helper data
	tableSpec        *metadata.TableSpecification
	preDeleteHooks   []ChangeHook[T]
	preHooks         []ChangeHook[T]
	postHooks        []ChangeHook[T]
	writeConsistency gocql.Consistency // Write consistency
}

// GetTableSpec gets the table specification we're using
func (t *tableManagerImpl[T]) GetTableSpec() *metadata.TableSpecification {
	return t.tableSpec.Clone(true)
}

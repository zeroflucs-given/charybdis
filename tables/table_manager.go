package tables

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/charybdis/metadata"

	"github.com/zeroflucs-given/generics"
)

// NewTableManager creates a table-manager instance
func NewTableManager[T any](ctx context.Context, options ...ManagerOption) (TableManager[T], error) {
	// Apply parameters
	params := tableManagerParameters{}
	params.ensureDefaults()
	for _, opt := range options {
		err := opt.mutateParameters(ctx, &params)
		if err != nil {
			return nil, fmt.Errorf("applying table manager option: %w", err)
		}
	}

	var extraOps []metadata.DDLOperation

	if params.TTL.Seconds() != 0 && params.TableSpec != nil {
		extraOps = append(extraOps, metadata.DDLOperation{
			Description:  "Add default TTL if non 0",
			Command:      fmt.Sprintf("alter table %s.%s with default_time_to_live = %v;", params.Keyspace, params.TableSpec.Name, int64(params.TTL.Seconds())),
			IgnoreErrors: []string{},
		})
	}

	// Execute hooks
	for _, opt := range options {
		err := opt.onStart(
			ctx,
			params.Keyspace,
			WithTableSpec(params.TableSpec),
			WithViewSpec(params.ViewSpec),
			WithTypeSpec(params.TypeSpecs...),
			WithAdditionalDDL(extraOps...),
		)
		if err != nil {
			return nil, fmt.Errorf("running table manager start hooks: %w", err)
		}
	}

	// Validate table spec
	errTable := params.TableSpec.Validate()
	if errTable != nil {
		return nil, fmt.Errorf("validating table spec: %w", errTable)
	}

	table, errTable := params.TableSpec.ToCQLX(params.Keyspace)
	if errTable != nil {
		return nil, fmt.Errorf("building table metadata: %w", errTable)
	}

	// Create our session
	wrappedSession, err := gocqlx.WrapSession(params.SessionFactory(params.Keyspace))
	if err != nil {
		return nil, fmt.Errorf("wrapping session: %w", err)
	}

	mgr := &tableManagerImpl[T]{
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
			qualifiedTableName: table.Name(),
			allColumnNames:     table.Metadata().Columns,
			nonKeyColumns: generics.Map(generics.Filter(params.TableSpec.Columns, func(i int, c *metadata.ColumnSpecification) bool {
				return !c.IsPartitioningKey && !c.IsClusteringKey
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
			queryTimeout: params.queryTimeout,
		},

		tableSpec:        params.TableSpec,
		writeConsistency: params.WriteConsistency,
	}

	if errCodec := mgr.adoptCodec(params.RowCodec); errCodec != nil {
		return nil, errCodec
	}
	// UDT columns need the reflection mapper's marshalling wrappers, which a codec bypasses
	if mgr.codec != nil && len(params.TableSpec.CustomTypes) > 0 {
		return nil, fmt.Errorf("row codec for %s not supported: table uses custom types, which require reflection mapping", params.TableSpec.Name)
	}
	mgr.precomputeStatements()
	mgr.upsertStmt, mgr.upsertNames = qb.Update(mgr.qualifiedTableName).
		Set(mgr.nonKeyColumns...).
		Where(mgr.allKeyPredicates...).
		ToCql()
	mgr.updateStmt, mgr.updateNames = qb.Update(mgr.qualifiedTableName).
		Set(mgr.nonKeyColumns...).
		Where(mgr.allKeyPredicates...).
		Existing().
		ToCql()
	mgr.insertStmt, mgr.insertNames = qb.Insert(mgr.qualifiedTableName).
		Columns(mgr.allColumnNames...).
		ToCql()
	mgr.insertNotExistsStmt, mgr.insertNotExistsNames = qb.Insert(mgr.qualifiedTableName).
		Columns(mgr.allColumnNames...).
		Unique().
		ToCql()

	return mgr, nil
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

	// Precomputed statements for option-free write paths
	upsertStmt           string
	upsertNames          []string
	updateStmt           string
	updateNames          []string
	insertStmt           string
	insertNames          []string
	insertNotExistsStmt  string
	insertNotExistsNames []string
}

// GetTableSpec gets the table specification we're using
func (t *tableManagerImpl[T]) GetTableSpec() *metadata.TableSpecification {
	return t.tableSpec.Clone(true)
}

func (t *tableManagerImpl[T]) GetSession() any {
	return t.Session
}

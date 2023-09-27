package tables

import (
	"context"
	"fmt"

	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/generics"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

// NewViewManager creates a view-manager instance. This is essentially a table-manager
// but without the insert/update/delete operations available.
func NewViewManager[T any](ctx context.Context, options ...ManagerOption) (ViewManager[T], error) {
	// Apply parameters
	params := tableManagerParameters{}
	params.ensureDefaults()
	for _, opt := range options {
		err := opt.mutateParameters(ctx, &params)
		if err != nil {
			return nil, fmt.Errorf("error applying view manager option: %w", err)
		}
	}

	// Execute hooks
	for _, opt := range options {
		err := opt.onStart(ctx, params.Keyspace, params.TableSpec, params.ViewSpec)
		if err != nil {
			return nil, fmt.Errorf("error running view manager start hooks: %w", err)
		}
	}

	// Validate view table spec
	errTable := params.TableSpec.Validate()
	if errTable != nil {
		return nil, fmt.Errorf("error validating view table spec: %w", errTable)
	}

	// Validate view spec
	errView := params.ViewSpec.Validate()
	if errView != nil {
		return nil, fmt.Errorf("error validating view spec: %w", errView)
	}

	// Create our session
	wrappedSession, err := gocqlx.WrapSession(params.SessionFactory(params.Keyspace))
	if err != nil {
		return nil, fmt.Errorf("error wrapping session: %w", err)
	}

	table := params.ViewSpec.ToCQLX()

	return &viewManager[T]{
		baseManagerImpl: baseManagerImpl[T]{
			// Base objects
			Logger: params.Logger.With(
				zap.String("keyspace", params.Keyspace),
				zap.String("view", params.ViewSpec.Name)),
			Tracer: otel.Tracer(TracingModuleName),
			DoTracing: params.DoTracing,

			// Metadata
			Name:          params.ViewSpec.Name,
			Session:       wrappedSession,
			Table:         table,
			TableMetadata: table.Metadata(),

			// Helper data
			readConsistency:    params.ReadConsistency,
			qualifiedTableName: params.Keyspace + "." + params.ViewSpec.Name,
			allColumnNames:     table.Metadata().Columns,
			partitionKeyPredicates: generics.Map(params.ViewSpec.Partitioning, func(i int, c *metadata.PartitioningColumn) qb.Cmp {
				return qb.Eq(c.Column.Name)
			}),
			allKeyPredicates: generics.Concatenate(
				generics.Map(params.ViewSpec.Partitioning, func(i int, p *metadata.PartitioningColumn) qb.Cmp {
					return qb.Eq(p.Column.Name)
				}),
				generics.Map(params.ViewSpec.Clustering, func(i int, c *metadata.ClusteringColumn) qb.Cmp {
					return qb.Eq(c.Column.Name)
				}),
			),
		},
	}, nil
}

type viewManager[T any] struct {
	baseManagerImpl[T]
}

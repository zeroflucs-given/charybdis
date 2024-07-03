package projections

import (
	"context"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/zeroflucs-given/charybdis/generator"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/tables"
	"github.com/zeroflucs-given/generics"
)

// NewProjectionManager creates a new projection manager. This provides a clean and simple way
// to manage N alternate representations of data laid out with different keys
func NewProjectionManager[T any](ctx context.Context,
	opts ...ProjectionManagerOption) (ProjectionManager[T], error) {
	// Build our options
	params := &projectionManagerParams{}
	params.ensureDefaults()
	for _, opt := range opts {
		opt.applyParams(params)
	}

	// The control table contains only the keys used for distribution
	controlSpec := buildControlTableSpec(params)
	controlManager, err := tables.NewTableManager[T](ctx,
		tables.WithCluster(params.cluster),
		tables.WithKeyspace(params.keyspace),
		tables.WithTableSpecification(controlSpec),
		generator.WithAutomaticTableManagement(params.logger, params.ddlClusterConfig),
	)
	if err != nil {
		return nil, fmt.Errorf("error initializing control table: %w", err)
	}

	// Build our projection objects
	projections := map[string]*projectionImpl[T]{}
	for i, proj := range params.projections {
		built, err := buildProjection[T](ctx, params, proj)
		if err != nil {
			return nil, fmt.Errorf("error configuring projection %d: %w", i, err)
		}
		if built != nil {
			projections[proj.Name] = built
		}
	}

	// The leaf table contains
	return &projectionManagerImpl[T]{
		controlTable: controlManager,
		naturalKeyEx: func(instance any) ([]any, error) {
			return extractPrimaryKey(controlSpec, instance)
		},
		projections: projections,
	}, nil
}

// projectionManagerImpl is our type that implements the projection manager
type projectionManagerImpl[T any] struct {
	controlTable tables.TableManager[T]        // The control-table that stores only the key data
	naturalKeyEx PrimaryKeyExtractor           // Function to extract primary key of base table
	projections  map[string]*projectionImpl[T] // N alternate projections
}

func buildControlTableSpec(params *projectionManagerParams) *metadata.TableSpecification {
	spec := params.baseTable.Clone(false)

	spec.Name += params.controlTableSuffix
	spec.Columns = generics.Filter(spec.Columns, func(i int, c *metadata.ColumnSpecification) bool {
		return c.IsPartitioningKey || c.IsClusteringKey || generics.Contains(params.nonKeyColumnsToTrack, c.Name)
	})

	return spec
}

// buildProjection builds an instance of the projection type
func buildProjection[T any](ctx context.Context, params *projectionManagerParams, spec *ProjectionSpecification) (*projectionImpl[T], error) {
	// Create the projection specification
	expectKeys := map[string]bool{}
	tableSpec := &metadata.TableSpecification{
		Name: spec.Name,
		Columns: generics.Map(params.baseTable.Columns, func(i int, c *metadata.ColumnSpecification) *metadata.ColumnSpecification {
			col := &metadata.ColumnSpecification{
				Name:    c.Name,
				CQLType: c.CQLType,
			}
			if c.IsPartitioningKey || c.IsClusteringKey {
				expectKeys[c.Name] = true
			}
			return col
		}),
	}
	colMap := map[string]*metadata.ColumnSpecification{}
	for _, c := range tableSpec.Columns {
		colMap[c.Name] = c
	}

	for i, cs := range spec.Partitioning {
		col, ok := colMap[cs.Column]
		if !ok {
			return nil, fmt.Errorf("missing column referenced in projection partitioning: %q", cs.Column)
		}
		col.IsPartitioningKey = true
		tableSpec.Partitioning = append(tableSpec.Partitioning, &metadata.PartitioningColumn{
			Column: col,
			Order:  i,
		})
		delete(expectKeys, cs.Column)
	}

	for i, cs := range spec.Clustering {
		col, ok := colMap[cs.Column]
		if !ok {
			return nil, fmt.Errorf("missing column referenced in projection clustering: %q", cs.Column)
		}
		col.IsClusteringKey = true
		tableSpec.Clustering = append(tableSpec.Clustering, &metadata.ClusteringColumn{
			Column:     col,
			Order:      i,
			Descending: cs.Descending,
		})
		delete(expectKeys, cs.Column)
	}
	tableSpec.Canonicalize()

	if len(expectKeys) > 0 {
		return nil, fmt.Errorf("the projection must include all keys - was missing %v", generics.Keys(expectKeys))
	}

	// Create the table-manager and configure the table
	tableManager, errManager := tables.NewTableManager[T](ctx,
		tables.WithCluster(params.cluster),
		tables.WithKeyspace(params.keyspace),
		tables.WithTableSpecification(tableSpec),
		generator.WithAutomaticTableManagement(params.logger, params.ddlClusterConfig),
	)
	if errManager != nil {
		return nil, fmt.Errorf("error building table manager for projection %v: %w", spec.Name, errManager)
	}

	return &projectionImpl[T]{
		projectionKeyEx: func(instance any) ([]any, error) {
			return extractPrimaryKey(tableSpec, instance)
		},
		leafTable: tableManager,
	}, nil
}

// Projection gets a projection by name
func (p *projectionManagerImpl[T]) Projection(name string) tables.ViewManager[T] {
	if proj, ok := p.projections[name]; ok {
		return proj.leafTable
	}
	return nil
}

// ProcessDelete performs the processing of a deleted object
func (p *projectionManagerImpl[T]) ProcessDelete(ctx context.Context, deleted *T) error {
	naturalKey, err := p.naturalKeyEx(deleted)
	if err != nil {
		return fmt.Errorf("error extracting projection manager control key: %w", err)
	}

	// Get the control table entry for this table and see if any of the control-values
	// are changed. If they are all the same, we can skip.
	ctrl, err := p.controlTable.GetByPrimaryKey(ctx, naturalKey...)
	if err != nil {
		return fmt.Errorf("error fetching control table record: %w", err)
	}

	// Remove the data from all projections
	grpCleanup, cleanupCtx := errgroup.WithContext(ctx)
	for _, p := range p.projections {
		proj := p
		grpCleanup.Go(func() error {
			return proj.Delete(cleanupCtx, ctrl)
		})
	}
	errCleanup := grpCleanup.Wait()
	if errCleanup != nil {
		return fmt.Errorf("error cleaning up projection tables before rewrite: %w", errCleanup)
	}

	errDeleteCtrl := p.controlTable.Delete(ctx, ctrl)
	if errDeleteCtrl != nil {
		return fmt.Errorf("error deleting control table record: %w", errDeleteCtrl)
	}

	return nil
}

// ProcessChange on a projection manager processes the incoming update.
func (p *projectionManagerImpl[T]) ProcessChange(ctx context.Context, updatedValue *T) error {
	naturalKey, err := p.naturalKeyEx(updatedValue)
	if err != nil {
		return fmt.Errorf("error extracting projection manager control key: %w", err)
	}

	// Get the control table entry for this table and see if any of the control-values
	// are changed. If they are all the same, we can skip.
	ctrl, err := p.controlTable.GetByPrimaryKey(ctx, naturalKey...)
	if err != nil {
		return fmt.Errorf("error fetching control table record: %w", err)
	}

	// Remove the data from all projections
	grpCleanup, cleanupCtx := errgroup.WithContext(ctx)
	for _, p := range p.projections {
		proj := p
		grpCleanup.Go(func() error {
			return proj.Delete(cleanupCtx, ctrl)
		})
	}
	errCleanup := grpCleanup.Wait()
	if errCleanup != nil {
		return fmt.Errorf("error cleaning up projection tables before rewrite: %w", errCleanup)
	}

	// Write the new control record
	errWriteCtrl := p.controlTable.Upsert(ctx, updatedValue)
	if errWriteCtrl != nil {
		return fmt.Errorf("error writing new control record: %w", errWriteCtrl)
	}

	// Now write all sub-tables
	grp, grpCtx := errgroup.WithContext(ctx)
	for _, p := range p.projections {
		proj := p
		grp.Go(func() error {
			return proj.ProcessChange(grpCtx, updatedValue)
		})
	}

	return grp.Wait()
}

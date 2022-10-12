package projections

import (
	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/metadata"
	"go.uber.org/zap"
)

// projectionManagerParams are the parameters we build for a projection manager
type projectionManagerParams struct {
	cluster              *gocql.ClusterConfig
	ddlClusterConfig     *gocql.ClusterConfig
	logger               *zap.Logger
	keyspace             string
	baseTable            *metadata.TableSpecification
	controlTableSuffix   string
	nonKeyColumnsToTrack []string
	projections          []*ProjectionSpecification
}

// ensureDefaults sets any default parameters
func (p *projectionManagerParams) ensureDefaults() {
	p.controlTableSuffix = "_ctrl"
	p.logger = zap.NewNop()
}

// ProjectionManagerOption is an option for our projection manager
type ProjectionManagerOption interface {
	applyParams(params *projectionManagerParams)
}

type projectionManagerOptionImpl struct {
	paramHook func(params *projectionManagerParams)
}

// applyParams applys any changes to parameters
func (p *projectionManagerOptionImpl) applyParams(params *projectionManagerParams) {
	if p != nil && p.paramHook != nil {
		p.paramHook(params)
	}
}

// WithKeyspace sets the keyspace to work with
func WithKeyspace(keyspace string) ProjectionManagerOption {
	return &projectionManagerOptionImpl{
		paramHook: func(params *projectionManagerParams) {
			params.keyspace = keyspace
		},
	}
}

// WithBaseTable sets the base table to work with
func WithBaseTable(spec *metadata.TableSpecification) ProjectionManagerOption {
	return &projectionManagerOptionImpl{
		paramHook: func(params *projectionManagerParams) {
			params.baseTable = spec
		},
	}
}

// WithCluster sets the cluster to use for the projection
func WithCluster(cluster *gocql.ClusterConfig) ProjectionManagerOption {
	return &projectionManagerOptionImpl{
		paramHook: func(params *projectionManagerParams) {
			params.cluster = cluster
			if params.ddlClusterConfig == nil {
				params.ddlClusterConfig = cluster
			}
		},
	}
}

// WithControlTableSuffix is the suffix used for the control table
func WithControlTableSuffix(suffix string) ProjectionManagerOption {
	return &projectionManagerOptionImpl{
		paramHook: func(params *projectionManagerParams) {
			params.controlTableSuffix = suffix
		},
	}
}

// WithLogger sets the logger to use
func WithLogger(logger *zap.Logger) ProjectionManagerOption {
	return &projectionManagerOptionImpl{
		paramHook: func(params *projectionManagerParams) {
			params.logger = logger
		},
	}
}

// WithSimpleProjection adds a simple projection to maintain
func WithSimpleProjection(spec *ProjectionSpecification) ProjectionManagerOption {
	return &projectionManagerOptionImpl{
		paramHook: func(params *projectionManagerParams) {
			params.projections = append(params.projections, spec)
		},
	}
}

// WithTrackedNonKeyColumns specifies the set of columns we track in the control table.
// You only need to specify the non-key columns to track here, if they form part of the projections
func WithTrackedNonKeyColumns(columns ...string) ProjectionManagerOption {
	return &projectionManagerOptionImpl{
		paramHook: func(params *projectionManagerParams) {
			params.nonKeyColumnsToTrack = columns
		},
	}
}

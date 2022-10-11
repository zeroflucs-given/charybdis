package tables

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/metadata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// WithCluster sets the cluster connection to use when working with the
// manager instance
func WithCluster(cluster *gocql.ClusterConfig) TableManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.SessionFactory = func(keyspace string) (*gocql.Session, error) {
				clusterVal := *cluster
				clusterVal.Keyspace = keyspace
				return clusterVal.CreateSession()
			}
			return nil
		},
	}
}

// WithTraceProvider sets the trace provider
func WithTraceProvider(provider trace.TracerProvider) TableManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.TracerProvider = provider
			return nil
		},
	}
}

// WithDefaultTTL is a table-manager option that sets the default TTL for inserts
// and updates.
func WithDefaultTTL(d time.Duration) TableManagerOption {
	ttlOpt := WithTTL(d)

	return &tableManagerOption{
		insertOpts: []InsertOption{ttlOpt},
		updateOpts: []UpdateOption{ttlOpt},
	}
}

// WithKeyspace sets the keyspace of the table-manager
func WithKeyspace(keyspace string) TableManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.Keyspace = keyspace
			return nil
		},
	}
}

// WithLogger sets the logger for the table manager
func WithLogger(log *zap.Logger) TableManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.Logger = log
			return nil
		},
	}
}

// WithTableSpecification sets the table specification to use
func WithTableSpecification(spec *metadata.TableSpecification) TableManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.TableSpec = spec
			return nil
		},
	}
}

// WithStartupFn attaches a function at startup time for the table-manager. This
// can for example be used to perform DDL/table specificiation maintainance.
func WithStartupFn(fn TableManagerStartupFn) TableManagerOption {
	return &tableManagerOption{
		startHook: fn,
	}
}

type tableParameterMutator func(ctx context.Context, params *tableManagerParameters) error

// TableManagerStartupFn is a startup function called before the table-manager is deemed ready to use.
type TableManagerStartupFn func(ctx context.Context, keyspace string, spec *metadata.TableSpecification) error

// TableManagerOption defines an option for the table manager
type TableManagerOption interface {
	mutateParameters(ctx context.Context, params *tableManagerParameters) error
	onStart(ctx context.Context, keyspace string, spec *metadata.TableSpecification) error
	insertOptions() []InsertOption
	updateOptions() []UpdateOption
	upsertOptions() []UpsertOption
}

type tableManagerOption struct {
	parametersHook tableParameterMutator
	startHook      TableManagerStartupFn
	insertOpts     []InsertOption
	updateOpts     []UpdateOption
	upsertOpts     []UpsertOption
}

// mutateParameters applies parameter mutations
func (t *tableManagerOption) mutateParameters(ctx context.Context, params *tableManagerParameters) error {
	if t.parametersHook == nil {
		return nil
	}

	return t.parametersHook(ctx, params)
}

func (t *tableManagerOption) onStart(ctx context.Context, keyspace string, spec *metadata.TableSpecification) error {
	if t.startHook == nil {
		return nil
	}

	return t.startHook(ctx, keyspace, spec)
}

func (t tableManagerOption) insertOptions() []InsertOption {
	return t.insertOpts
}

func (t tableManagerOption) updateOptions() []UpdateOption {
	return t.updateOpts
}

func (t tableManagerOption) upsertOptions() []UpsertOption {
	return t.upsertOpts
}

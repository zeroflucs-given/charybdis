package tables

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/utils"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// WithCluster sets the cluster connection to use when working with the
// manager instance
func WithCluster(cluster utils.ClusterConfigGeneratorFn) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.SessionFactory = func(keyspace string) (*gocql.Session, error) {
				clusterVal := cluster()
				clusterVal.Keyspace = keyspace
				return clusterVal.CreateSession()
			}
			return nil
		},
	}
}

// WithDefaultReadConsistency sets the default read consistency
func WithDefaultReadConsistency(level gocql.Consistency) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.ReadConsistency = level
			return nil
		},
	}
}

// WithDefaultWriteConsistency sets the default read consistency
func WithDefaultWriteConsistency(level gocql.Consistency) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.WriteConsistency = level
			return nil
		},
	}
}

// WithTraceProvider sets the trace provider
func WithTraceProvider(provider trace.TracerProvider) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.TracerProvider = provider
			params.DoTracing = true
			return nil
		},
	}
}

// WithDefaultTTL is a table-manager option that sets the default TTL for inserts
// and updates.
func WithDefaultTTL(d time.Duration) ManagerOption {
	ttlOpt := WithTTL(d)

	return &tableManagerOption{
		insertOpts: []InsertOption{ttlOpt},
		updateOpts: []UpdateOption{ttlOpt},
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.TTL = d
			return nil
		},
	}
}

// WithKeyspace sets the keyspace of the table-manager
func WithKeyspace(keyspace string) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.Keyspace = keyspace
			return nil
		},
	}
}

// WithLogger sets the logger for the table manager
func WithLogger(log *zap.Logger) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.Logger = log
			return nil
		},
	}
}

// WithTableSpecification sets the table specification to use
func WithTableSpecification(spec *metadata.TableSpecification) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.TableSpec = spec
			return nil
		},
	}
}

// WithViewSpecification sets the view specification to use
func WithViewSpecification(spec *metadata.ViewSpecification) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.ViewSpec = spec
			return nil
		},
	}
}

// WithStartupFn attaches a function at startup time for the table-manager. This
// can for example be used to perform DDL/table specificiation maintainance.
func WithStartupFn(fn TableManagerStartupFn) ManagerOption {
	return &tableManagerOption{
		startHook: fn,
	}
}

type SpecMutator func(ctx context.Context, table *metadata.TableSpecification, view *metadata.ViewSpecification) (*metadata.TableSpecification, *metadata.ViewSpecification, error)

// WithSpecMutator mutates the table/view specifications on startup
func WithSpecMutator(mutator SpecMutator) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			t, v, err := mutator(ctx, params.TableSpec, params.ViewSpec)
			params.TableSpec = t
			params.ViewSpec = v
			return err
		},
	}
}

type tableParameterMutator func(ctx context.Context, params *tableManagerParameters) error

// TableManagerStartupFn is a startup function called before the table-manager is deemed ready to use.
type TableManagerStartupFn func(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error

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

func (t *tableManagerOption) onStart(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error {
	if t.startHook == nil {
		return nil
	}

	return t.startHook(ctx, keyspace, table, view, extraOps...)
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

func (t tableManagerOption) beforeChange(ctx context.Context, rec any) error {
	return nil
}

func (t tableManagerOption) afterChange(ctx context.Context, rec any) error {
	return nil
}

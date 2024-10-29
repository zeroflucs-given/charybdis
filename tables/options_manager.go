package tables

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/utils"
)

// WithCluster sets the cluster connection to use when working with the
// manager instance
func WithCluster(cluster utils.ClusterConfigGeneratorFn) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.SessionFactory = func(keyspace string) (*gocql.Session, error) {
				clusterVal := cluster()
				clusterVal.Keyspace = keyspace
				params.queryTimeout = clusterVal.Timeout
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
			params.TypeSpecs = append(params.TypeSpecs, spec.CustomTypes...)
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

// WithTypeSpecification sets one or more type specifications to use
func WithTypeSpecification(spec ...*metadata.TypeSpecification) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.TypeSpecs = append(params.TypeSpecs, spec...)
			return nil
		},
	}
}

// WithStartupFn attaches a function at startup time for the table-manager.
// This can for example be used to perform DDL/table specification maintenance.
//
// Deprecated: Prefer WithStartupFnEx as it supports custom types and any future features.
func WithStartupFn(fn TableManagerStartupFn) ManagerOption {
	return &tableManagerOption{
		startHook: fn,
	}
}

// WithStartupFnEx attaches a function at startup time for the table-manager.
// This provides the same functionality as WithStartupFn, while allowing later extension more easily
func WithStartupFnEx(fn TableManagerStartupFnEx) ManagerOption {
	return &tableManagerOption{
		startHookEx: fn,
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
	startHookEx    TableManagerStartupFnEx
	insertOpts     []InsertOption
	deleteOpts     []DeleteOption
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

func (t *tableManagerOption) onStart(ctx context.Context, keyspace string, options ...StartupOption) error {
	var err error

	if t.startHook != nil {
		opts := CollectStartupOptions(options)
		err = t.startHook(ctx, keyspace, opts.table, opts.view, opts.ddl...)
	}
	if err != nil {
		return err
	}

	if t.startHookEx != nil {
		err = t.startHookEx(ctx, keyspace, options...)
	}

	return err
}

func (t *tableManagerOption) insertOptions() []InsertOption {
	return t.insertOpts
}

func (t *tableManagerOption) deleteOptions() []DeleteOption {
	return t.deleteOpts
}

func (t *tableManagerOption) updateOptions() []UpdateOption {
	return t.updateOpts
}

func (t *tableManagerOption) upsertOptions() []UpsertOption {
	return t.upsertOpts
}

func (t *tableManagerOption) beforeChange(ctx context.Context, rec any) error {
	return nil
}

func (t *tableManagerOption) afterChange(ctx context.Context, rec any) error {
	return nil
}

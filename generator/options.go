package generator

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/tables"
	"github.com/zeroflucs-given/charybdis/utils"
)

// WithAutomaticTableManagement automatically performs management of tables and structures on startup
// when using a tables.TableManager, using the charybdis DDL generator.
func WithAutomaticTableManagement(log *zap.Logger, clusterFn utils.ClusterConfigGeneratorFn) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, spec *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error {
		if view != nil {
			return fmt.Errorf("should not have a view during startup: %q", view.Name)
		}

		cluster := clusterFn()

		sess, err := gocqlx.WrapSession(cluster.CreateSession())
		if err != nil {
			return fmt.Errorf("error creating table management session: %w", err)
		}
		defer sess.Close()

		return installTableFromDDL(ctx, log, sess, keyspace, spec, extraOps...)
	})
}

// installTableFromDDL performs the underlying installation of the table, including progressively adding
// any new columns.
func installTableFromDDL(ctx context.Context, logger *zap.Logger, sess gocqlx.Session, keyspace string, spec *metadata.TableSpecification, extraOps ...metadata.DDLOperation) error {
	logger.Info("Starting table installation")
	defer logger.Info("Finished table installation")

	if spec == nil {
		return fmt.Errorf("invalid table spec")
	}

	// Get the existing state of the keyspace
	existing, errDef := DescribeTableMetadata(sess, keyspace, spec.Name)
	if errDef != nil {
		return fmt.Errorf("error reading table metadata: %w", errDef)
	}

	statements, err := CreateDDLFromTableSpecification(keyspace, spec, existing)
	if err != nil {
		return fmt.Errorf("error creating table DDL: %w", err)
	}

	statements = append(statements, extraOps...)

	return installDLL(ctx, logger, sess, statements)
}

// WithAutomaticViewManagement automatically performs management of views and structures on startup
// when using a tables.ViewManager, using the charybdis DDL generator.
func WithAutomaticViewManagement(log *zap.Logger, cluster utils.ClusterConfigGeneratorFn) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error {
		if view == nil {
			return fmt.Errorf("should have a view during startup for table %q", table.Name)
		}

		sess, err := gocqlx.WrapSession(cluster().CreateSession())
		if err != nil {
			return fmt.Errorf("error creating table management session: %w", err)
		}
		defer sess.Close()

		return installViewFromDDL(ctx, log, sess, keyspace, view)
	})
}

// installTableFromDDL performs the underlying installation of the table, including progressively adding
// any new columns.
func installViewFromDDL(ctx context.Context, logger *zap.Logger, sess gocqlx.Session, keyspace string, spec *metadata.ViewSpecification) error {
	logger.Info("Starting view installation")
	defer logger.Info("Finished view installation")

	if spec == nil {
		return fmt.Errorf("invalid view spec")
	}

	// Get the existing state of the keyspace
	existing, errDef := DescribeViewMetadata(sess, keyspace, spec.Name)
	if errDef != nil {
		return fmt.Errorf("error reading view metadata: %w", errDef)
	}

	statements, err := CreateDDLFromViewSpecification(keyspace, spec, existing)
	if err != nil {
		return fmt.Errorf("error creating view DDL: %w", err)
	}

	return installDLL(ctx, logger, sess, statements)
}

// WithKeyspaceManagement does a 'CREATE KEYSPACE' command at the startup, with replication factor and other options passed in as args.
func WithKeyspaceManagement(log *zap.Logger, cluster utils.ClusterConfigGeneratorFn, options ...KeyspaceOption) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	opts := CollectKeyspaceOptions(options)

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error {
		sess, err := gocqlx.WrapSession(cluster().CreateSession())
		if err != nil {
			return fmt.Errorf("keyspace management session: %w", err)
		}
		defer sess.Close()

		keyspaceMetadata, errMetadata := DescribeKeyspaceMetadata(sess, keyspace)
		if errMetadata != nil {
			return fmt.Errorf("reading existing keyspace metadata: %w", errMetadata)
		}
		if keyspaceMetadata != nil {
			return nil // Keyspace already exists
		}

		err = CreateKeyspace(ctx, sess, keyspace, UsingOptions(opts), UsingLogger(log))
		if err != nil {
			return fmt.Errorf("creating keyspace %q: %w", keyspace, err)
		}

		return nil
	})
}

// WithSimpleKeyspaceManagement does a 'CREATE KEYSPACE' command at the startup, with a default replication factor.
// This should only be used for trivial scenarios.
func WithSimpleKeyspaceManagement(log *zap.Logger, cluster utils.ClusterConfigGeneratorFn, replicationFactor int) tables.ManagerOption {
	return WithKeyspaceManagement(log, cluster, UsingReplicationFactor(replicationFactor))
}

// WithNetworkAwareKeyspaceManagement creates a network aware keyspace
func WithNetworkAwareKeyspaceManagement(log *zap.Logger, cluster utils.ClusterConfigGeneratorFn, replicationFactors map[string]int32) tables.ManagerOption {
	return WithKeyspaceManagement(log, cluster, UsingNetworkReplicationFactors(replicationFactors))
}

// WithGlobalNetworkAwareKeyspaceManagement creates a network aware keyspace with global network aware replication.
// This is the equivalent of using WithNetworkAwareKeyspaceManagement with the same replication for every DC in the cluster.
func WithGlobalNetworkAwareKeyspaceManagement(log *zap.Logger, cluster utils.ClusterConfigGeneratorFn, replicationFactor int) tables.ManagerOption {
	return WithKeyspaceManagement(log, cluster, UsingNetworkReplication(replicationFactor))
}

func installDLL(ctx context.Context, logger *zap.Logger, sess gocqlx.Session, statements []metadata.DDLOperation) error {
outer:
	for _, statement := range statements {
		logger.With(zap.String("query", statement.Command)).Info(statement.Description)
		errRun := sess.ContextQuery(ctx, statement.Command, nil).ExecRelease()

		// Check if there's an error, and ensure its one that we're allowed to see.
		// TODO: Can we use error codes for "column already exists" or otherwise maintain
		// the tables using schema introspection?
		if errRun != nil {
			errMsg := errRun.Error()

			for _, expect := range statement.IgnoreErrors {
				if strings.Contains(errMsg, expect) {
					continue outer
				}
			}

			return fmt.Errorf("error_running %q: %w", statement.Command, errRun)
		}
	}

	return nil
}

type tableMetadata struct {
	Table   *gocql.TableMetadata
	Indexes map[string]*gocql.IndexMetadata
}

// DescribeTableMetadata reads the schema of a table in a given keyspace schema from the database
func DescribeTableMetadata(sess gocqlx.Session, keyspace string, tableName string) (*tableMetadata, error) {

	keyspaceMetadata, errDef := sess.KeyspaceMetadata(keyspace)
	if errors.Is(errDef, gocql.ErrKeyspaceDoesNotExist) {
		return nil, nil
	} else if errDef != nil {
		return nil, fmt.Errorf("error fetching keyspace metadata: %w", errDef)
	}
	var table *gocql.TableMetadata
	if keyspaceMetadata.Tables != nil {
		table = keyspaceMetadata.Tables[tableName]
	}

	indexMetadata := map[string]*gocql.IndexMetadata{}
	if keyspaceMetadata.Indexes != nil {
		for iName, i := range keyspaceMetadata.Indexes {
			if i.TableName == tableName {
				indexMetadata[iName] = i
			}
		}
	}

	return &tableMetadata{
		Table:   table,
		Indexes: indexMetadata,
	}, nil
}

// DescribeViewMetadata reads the schema of a view in a given keyspace schema from the database
func DescribeViewMetadata(sess gocqlx.Session, keyspace string, viewName string) (*gocql.ViewMetadata, error) {
	md, errDef := sess.KeyspaceMetadata(keyspace)
	if errors.Is(errDef, gocql.ErrKeyspaceDoesNotExist) {
		return nil, nil
	} else if errDef != nil {
		return nil, fmt.Errorf("error fetching keyspace metadata: %w", errDef)
	}
	if md.Views != nil {
		return md.Views[viewName], nil
	}
	return nil, nil
}

// DescribeKeyspaceMetadata reads the schema of a keyspace from the database
func DescribeKeyspaceMetadata(sess gocqlx.Session, keyspace string) (*gocql.KeyspaceMetadata, error) {
	md, errDef := sess.KeyspaceMetadata(keyspace)
	if errors.Is(errDef, gocql.ErrKeyspaceDoesNotExist) {
		return nil, nil
	} else if errDef != nil {
		return nil, fmt.Errorf("error fetching keyspace metadata: %w", errDef)
	}
	return md, nil
}

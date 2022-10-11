package generator

import (
	"context"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/tables"
	"go.uber.org/zap"
)

// WithAutomaticTableManagement automatically performs management of tables and structures on startup
// when using a tables.TableManager, using the charybdis DDL generator.
func WithAutomaticTableManagement(log *zap.Logger, cluster *gocql.ClusterConfig) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, spec *metadata.TableSpecification, view *metadata.ViewSpecification) error {
		if view != nil {
			return fmt.Errorf("should not have a view during startup: %q", view.Name)
		}

		sess, err := gocqlx.WrapSession(cluster.CreateSession())
		if err != nil {
			return fmt.Errorf("error creating table management session: %w", err)
		}
		defer sess.Close()

		return installTableFromDDL(ctx, log, sess, keyspace, spec)
	})
}

// installTableFromDDL performs the underlying installation of the table, including progressively adding
// any new columns.
func installTableFromDDL(ctx context.Context, logger *zap.Logger, sess gocqlx.Session, keyspace string, spec *metadata.TableSpecification) error {
	logger.Info("Starting table installation")
	defer logger.Info("Finished table installation")

	statements, err := CreateDDLFromTableSpecification(keyspace, spec)
	if err != nil {
		return fmt.Errorf("error creating table DDL: %w", err)
	}

	return installDLL(ctx, logger, sess, statements)
}

// WithAutomaticViewManagement automatically performs management of views and structures on startup
// when using a tables.ViewManager, using the charybdis DDL generator.
func WithAutomaticViewManagement(log *zap.Logger, cluster *gocql.ClusterConfig) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification) error {
		if view == nil {
			return fmt.Errorf("should have a view during startup for table %q", table.Name)
		}

		sess, err := gocqlx.WrapSession(cluster.CreateSession())
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

	statements, err := CreateDDLFromViewSpecification(keyspace, spec)
	if err != nil {
		return fmt.Errorf("error creating view DDL: %w", err)
	}

	return installDLL(ctx, logger, sess, statements)
}

// WithSimpleKeyspaceManagement does a 'CREATE KEYSPACE' command at the startup, with a default replication
// factor. This should only be used for trivial scenarios.
func WithSimpleKeyspaceManagement(log *zap.Logger, cluster *gocql.ClusterConfig, replicationFactor int) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification) error {
		sess, err := gocqlx.WrapSession(cluster.CreateSession())
		if err != nil {
			return fmt.Errorf("error keyspace management session: %w", err)
		}
		defer sess.Close()

		stmt := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS  %s WITH replication = {
			'class' : 'SimpleStrategy',
			'replication_factor' : %d
		}`, keyspace, replicationFactor)

		log.With(zap.String("query", stmt)).Info("Creating keyspace, if required with simple replication.")

		return sess.ExecStmt(stmt)
	})
}

func installDLL(ctx context.Context, logger *zap.Logger, sess gocqlx.Session, statements []DDLOperation) error {
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

package generator

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/tables"
	"github.com/zeroflucs-given/charybdis/utils"
	"github.com/zeroflucs-given/generics"
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

// WithKeyspaceManagement does a 'CREATE KEYSPACE' command at the startup, with a default replication factor. This should only be used for trivial scenarios.
func WithKeyspaceManagement(log *zap.Logger, cluster utils.ClusterConfigGeneratorFn, options ...KeyspaceOption) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	opts := CollectKeyspaceOptions(options)

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error {
		sess, err := gocqlx.WrapSession(cluster().CreateSession())
		if err != nil {
			return fmt.Errorf("error keyspace management session: %w", err)
		}
		defer sess.Close()

		keyspaceMetadata, errMetadata := DescribeKeyspaceMetadata(sess, keyspace)
		if errMetadata != nil {
			return fmt.Errorf("error reading existing keyspace metadata: %w", errMetadata)
		}
		if keyspaceMetadata != nil {
			return nil // Keyspace already exists
		}

		var keyspaceOptionClauses []string

		if opts.isNetwork {
			keyspaceOptionClauses = append(keyspaceOptionClauses, fmt.Sprintf("replication = { 'class': 'NetworkTopologyStrategy', %v }", strings.Join(opts.replicationFactors, ", ")))
		} else {
			keyspaceOptionClauses = append(keyspaceOptionClauses, fmt.Sprintf("replication = { 'class': 'SimpleStrategy', 'replication_factor': %d }", opts.replicationFactor))
		}

		version, err := GetScyllaVersion(ctx, sess)
		if err != nil {
			return err
		}

		if version.Major >= 6 { // Tablets exist as of Scylla v6. An unspecified `tablets` clause enables them. We default to disabled instead
			keyspaceOptionClauses = append(keyspaceOptionClauses, fmt.Sprintf("tablets = { 'enabled': %t }", opts.enableTablets))
		}

		with := strings.Join(keyspaceOptionClauses, " AND ")
		if len(with) > 0 {
			with = " WITH " + with
		}

		stmt := fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %s%s", keyspace, with)

		log.With(zap.String("query", stmt), zap.Any("scylla_version", version)).Info("Creating keyspace")

		return sess.ExecStmt(stmt)
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

// WithNetworkAwareKeyspaceManagement creates a network aware keyspace with global network aware replication
// This is the equivalent of using WithNetworkAwareKeyspaceManagement with the same replication for every DC in the cluster
func WithGlobalNetworkAwareKeyspaceManagement(log *zap.Logger, cluster utils.ClusterConfigGeneratorFn, replicationFactor int) tables.ManagerOption {
	if log == nil {
		log = zap.NewNop()
	}

	return tables.WithStartupFn(func(ctx context.Context, keyspace string, table *metadata.TableSpecification, view *metadata.ViewSpecification, extraOps ...metadata.DDLOperation) error {
		sess, err := gocqlx.WrapSession(cluster().CreateSession())
		if err != nil {
			return fmt.Errorf("error keyspace management session: %w", err)
		}
		defer sess.Close()

		keyspaceMetadata, errMetadata := DescribeKeyspaceMetadata(sess, keyspace)
		if errMetadata != nil {
			return fmt.Errorf("error reading existing keyspace metadata: %w", errMetadata)
		}
		if keyspaceMetadata != nil {
			return nil // Keyspace already exists
		}

		stmt := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS  %s WITH replication = {
			'class' : 'NetworkTopologyStrategy',
			'replication_factor' : %d		
		}`, keyspace, replicationFactor)

		log.With(zap.String("query", stmt)).Info("Creating keyspace, if required with global network aware replication.")

		return sess.ExecStmt(stmt)
	})
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

type Version struct {
	Major int
	Minor int
	Patch int
	Tag   string
}

func GetScyllaVersion(ctx context.Context, sess gocqlx.Session) (Version, error) {
	v := Version{}

	var version string
	err := sess.ContextQuery(ctx, "SELECT version FROM system.versions", nil).Consistency(gocql.One).Get(&version)
	if err != nil {
		return v, err
	}

	return ParseVersion(version), nil
}

func ParseVersion(version string) Version {
	v := Version{}

	parts := strings.SplitN(version, ".", 3)

	parseValue := func(part string) (int, string) {
		var t string

		p := strings.SplitN(part, "-", 2)
		if len(p) == 0 {
			return 0, ""
		}

		i, e := strconv.ParseInt(p[0], 10, 32)
		if e != nil {
			return 0, p[0]
		}

		if len(p) > 1 {
			t = p[1]
		}

		return int(i), t
	}

	var t string
	switch len(parts) {
	case 3:
		v.Patch, t = parseValue(parts[2])
		if t != "" {
			v.Tag = t
		}
		fallthrough
	case 2:
		v.Minor, t = parseValue(parts[1])
		if t != "" {
			v.Tag = t
		}
		fallthrough
	case 1:
		v.Major, t = parseValue(parts[0])
		if t != "" {
			v.Tag = t
		}
	}

	return v
}

type KeyspaceOption func(*KeyspaceOptions)

type KeyspaceOptions struct {
	isNetwork          bool
	replicationFactor  int
	replicationFactors []string
	replicationMap     map[string]int32
	enableTablets      bool
}

func CollectKeyspaceOptions(opts []KeyspaceOption) KeyspaceOptions {
	o := KeyspaceOptions{}
	for _, fn := range opts {
		fn(&o)
	}
	if o.isNetwork {
		o.replicationFactors = generics.Map(generics.ToKeyValues(o.replicationMap), func(i int, kvp generics.KeyValuePair[string, int32]) string {
			return fmt.Sprintf("'%v': %d", kvp.Key, kvp.Value)
		})
		sort.Strings(o.replicationFactors)
	}
	return o
}

func UsingTablets() KeyspaceOption {
	return func(o *KeyspaceOptions) {
		o.enableTablets = true
	}
}

func UsingReplicationFactor(factor int) KeyspaceOption {
	return func(o *KeyspaceOptions) {
		o.replicationFactor = factor
	}
}

func UsingNetworkReplicationFactors(factors map[string]int32) KeyspaceOption {
	return func(o *KeyspaceOptions) {
		o.isNetwork = true
		o.replicationMap = factors
	}
}

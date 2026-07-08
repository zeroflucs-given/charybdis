package generator

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/generics"

	"github.com/zeroflucs-given/charybdis/metadata"
)

type KeyspaceOption func(*KeyspaceOptions)

type KeyspaceOptions struct {
	isNetwork          bool
	replicationFactor  int
	replicationFactors []string
	replicationMap     map[string]int32
	enableTablets      bool
	logger             *zap.Logger
}

func CollectKeyspaceOptions(opts []KeyspaceOption) KeyspaceOptions {
	// home defaults
	o := KeyspaceOptions{
		replicationFactor: 1, // A factor of 1 should only be used in testing, never prod
	}
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

func UsingOptions(from KeyspaceOptions) KeyspaceOption {
	return func(to *KeyspaceOptions) {
		to.isNetwork = from.isNetwork
		to.replicationFactor = from.replicationFactor
		to.replicationFactors = slices.Clone(from.replicationFactors)
		to.replicationMap = maps.Clone(from.replicationMap)
		to.enableTablets = from.enableTablets
		to.logger = from.logger
	}
}

func UsingTablets(enable bool) KeyspaceOption {
	return func(o *KeyspaceOptions) {
		o.enableTablets = enable
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

func UsingNetworkReplication(factor int) KeyspaceOption {
	return func(o *KeyspaceOptions) {
		o.isNetwork = true
		o.replicationFactor = factor
	}
}

func UsingLogger(logger *zap.Logger) KeyspaceOption {
	return func(o *KeyspaceOptions) {
		o.logger = logger
	}
}

func CreateKeyspace(ctx context.Context, sess gocqlx.Session, keyspace string, options ...KeyspaceOption) error {
	logger := zap.NewNop()
	opts := CollectKeyspaceOptions(options)
	if opts.logger != nil {
		logger = opts.logger
	}
	gen := &DefinitionGenerator{
		logger:  logger,
		session: sess,
	}
	return gen.CreateKeyspace(ctx, keyspace, options...)
}

func (g *DefinitionGenerator) CreateKeyspace(ctx context.Context, keyspace string, options ...KeyspaceOption) error {
	opts := CollectKeyspaceOptions(options)

	if err := IsValidIdentifier(keyspace); err != nil {
		return fmt.Errorf("invalid name for a keyspace: %w", err)
	}

	var keyspaceOptionClauses []string

	replicationStrategy := "SimpleStrategy"
	if opts.isNetwork {
		replicationStrategy = "NetworkTopologyStrategy"
	}

	if len(opts.replicationFactors) > 0 {
		keyspaceOptionClauses = append(keyspaceOptionClauses, fmt.Sprintf("replication = { 'class': '%s', %s }", replicationStrategy, strings.Join(opts.replicationFactors, ", ")))
	} else {
		replicationFactor := opts.replicationFactor
		if replicationFactor == 0 {
			replicationFactor = 1
		}
		keyspaceOptionClauses = append(keyspaceOptionClauses, fmt.Sprintf("replication = { 'class': '%s', 'replication_factor': %d }", replicationStrategy, replicationFactor))
	}

	version, err := metadata.GetScyllaVersion(ctx, g.session)
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

	g.logger.With(zap.String("query", stmt), zap.Any("scylla_version", version)).Info("Creating keyspace")

	return g.session.ContextQuery(ctx, stmt, nil).ExecRelease()
}

func (g *DefinitionGenerator) DropKeyspace(ctx context.Context, keyspace string) error {
	ddl, errDDL := DropKeyspaceDDL(keyspace)
	if errDDL != nil {
		return errDDL
	}
	return installDLL(ctx, g.logger, g.session, ddl)
}

func DropKeyspace(ctx context.Context, sess gocqlx.Session, keyspace string) error {
	gen := &DefinitionGenerator{
		logger:  zap.NewNop(),
		session: sess,
	}
	return gen.DropKeyspace(ctx, keyspace)
}

func DropKeyspaceDDL(keyspace string) ([]metadata.DDLOperation, error) {
	if err := IsValidIdentifier(keyspace); err != nil {
		return nil, fmt.Errorf("invalid keyspace name: %w", err)
	}

	var commands []metadata.DDLOperation
	commands = append(commands, metadata.DDLOperation{
		Description: fmt.Sprintf("Drop the keyspace '%s'", keyspace),
		Command:     fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace),
	})

	return commands, nil
}

package generator

import (
	"context"
	"fmt"
	"github.com/scylladb/gocqlx/v2"
	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/generics"
	"go.uber.org/zap"
	"maps"
	"slices"
	"sort"
	"strings"
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
	opts := CollectKeyspaceOptions(options)

	var keyspaceOptionClauses []string

	replicationStrategy := "SimpleStrategy"
	if opts.isNetwork {
		replicationStrategy = "NetworkTopologyStrategy"
	}

	if len(opts.replicationFactors) > 0 {
		keyspaceOptionClauses = append(keyspaceOptionClauses, fmt.Sprintf("replication = { 'class': '%s', %s }", replicationStrategy, strings.Join(opts.replicationFactors, ", ")))
	} else if opts.replicationFactor > 0 {
		keyspaceOptionClauses = append(keyspaceOptionClauses, fmt.Sprintf("replication = { 'class': '%s', 'replication_factor': %d }", replicationStrategy, opts.replicationFactor))
	}

	version, err := metadata.GetScyllaVersion(ctx, sess)
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

	if opts.logger != nil {
		opts.logger.With(zap.String("query", stmt), zap.Any("scylla_version", version)).Info("Creating keyspace")
	}

	return sess.ExecStmt(stmt)
}

package generator

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testTableDeclarations = []string{
	"DROP KEYSPACE IF EXISTS charybdis_tests",
	"CREATE KEYSPACE charybdis_tests WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}",
	"CREATE TYPE charybdis_tests.address (number varchar, street text, city varchar)",
	"create table charybdis_tests.orders (order_id varchar, shipping_address address, primary key(order_id))",
	"create table charybdis_tests.order_items (order_id varchar, item_id varchar, quantity int, primary key((order_id), item_id))",
	"CREATE INDEX order_item_lookup ON charybdis_tests.order_items (item_id)",
	"CREATE MATERIALIZED VIEW charybdis_tests.item_orders AS SELECT * FROM charybdis_tests.order_items WHERE order_id IS NOT NULL AND item_id IS NOT NULL AND (quantity > 0) PRIMARY KEY((item_id), order_id, quantity) WITH CLUSTERING ORDER BY (order_id ASC)",
}

func getTestClusterConfig() gocql.ClusterConfig {
	testHosts := []string{"localhost:9042", "localhost:9042"}

	cfg := gocql.NewCluster(testHosts...)
	cfg.DisableInitialHostLookup = true
	// cfg.Authenticator = gocql.PasswordAuthenticator{
	// 	Username: "cassandra",
	// 	Password: "cassandra",
	// }
	cfg.Consistency = gocql.Quorum
	cfg.SslOpts = nil
	return *cfg
}

func DisabledTestCreateRole(t *testing.T) {
	cfg := getTestClusterConfig()

	sess, err := gocqlx.WrapSession(cfg.CreateSession())
	// sess, err := gocqlx.WrapSession(gocql.NewSession(cfg))

	require.NoError(t, err)
	defer sess.Close()

	for _, statement := range testTableDeclarations {
		errStmt := sess.ExecStmt(statement)
		require.NoError(t, errStmt)
	}

	err = CreateRoleX("foo").With("PASSWORD", "'foo'").Exec(sess)
	assert.NoError(t, err, "expected role to be created")

}

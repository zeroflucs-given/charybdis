package tables_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/zeroflucs-given/charybdis/metadata"
)

// TestMain initializes our testing setup and installs any tables/keyspaces we use for testing.
// It assumes absolute ownership of the keyspace charybdis_tests.
func TestMain(m *testing.M) {
	testHosts := []string{"localhost:9042"}
	testClusterConfig = gocql.NewCluster(testHosts...)

	// Create the test schema and tables
	sess, err := gocqlx.WrapSession(gocql.NewSession(*testClusterConfig))
	if err != nil {
		panic(err)
	}
	for _, statement := range testTableDeclarations {
		err := sess.ExecStmt(statement)
		if err != nil {
			panic(err)
		}
	}

	m.Run()
}

const TestKeyspace = "charybdis_tests"

// testTableDeclarations is our keyspace definition and schema changes
var testTableDeclarations = []string{
	"DROP KEYSPACE IF EXISTS charybdis_tests",
	"CREATE KEYSPACE charybdis_tests WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}",
	"CREATE TABLE charybdis_tests.orders (order_id varchar, shipping_address varchar, PRIMARY KEY(order_id))",
	"CREATE TABLE charybdis_tests.order_items (order_id varchar, item_id varchar, quantity int, PRIMARY KEY((order_id), item_id))",
	"CREATE INDEX order_item_lookup ON charybdis_tests.order_items (item_id)",
}

var testClusterConfig *gocql.ClusterConfig

// Orders table
var orderColumns = []*metadata.ColumnSpecification{
	{
		Name:              "order_id",
		CQLType:           "varchar",
		IsPartitioningKey: true,
	},
	{
		Name:    "shipping_address",
		CQLType: "varchar",
	},
}
var OrdersTableSpec = &metadata.TableSpecification{
	Name: "orders",
	Columns: []*metadata.ColumnSpecification{
		orderColumns[0],
		orderColumns[1],
	},
	Partitioning: []*metadata.PartitioningColumn{
		{
			Column: orderColumns[0],
			Order:  1,
		},
	},
	Clustering: []*metadata.ClusteringColumn{},
}

// Order Items table
var orderItemColumns = []*metadata.ColumnSpecification{
	{
		Name:              "order_id",
		CQLType:           "varchar",
		IsPartitioningKey: true,
	},
	{
		Name:            "item_id",
		CQLType:         "varchar",
		IsClusteringKey: true,
	},
	{
		Name:    "quantity",
		CQLType: "int",
	},
}
var OrderItemsTableSpec = &metadata.TableSpecification{
	Name: "order_items",
	Columns: []*metadata.ColumnSpecification{
		orderItemColumns[0],
		orderItemColumns[1],
		orderItemColumns[2],
	},
	Partitioning: []*metadata.PartitioningColumn{
		{
			Column: orderItemColumns[0],
			Order:  1,
		},
	},
	Clustering: []*metadata.ClusteringColumn{
		{
			Column:     orderItemColumns[1],
			Order:      1,
			Descending: false,
		},
	},
}

type Order struct {
	OrderID         string `cql:"order_id"`
	ShippingAddress string `cql:"shipping_address"`
}

type OrderItem struct {
	OrderID  string `cql:"order_id"`
	ItemID   string `cql:"item_id"`
	Quantity int    `cql:"quantity"`
}

package tables_test

import (
	"slices"
	"strconv"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/utils"
)

// TestMain initializes our testing setup and installs any tables/keyspaces we use for testing.
// It assumes absolute ownership of the keyspace charybdis_tests.
func TestMain(m *testing.M) {
	testHosts := []string{"localhost:9042"}
	testClusterConfig = func() *gocql.ClusterConfig {
		return gocql.NewCluster(testHosts...)
	}

	instance := testClusterConfig()

	// Create the test schema and tables
	sess, err := gocqlx.WrapSession(gocql.NewSession(*instance))
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
	"CREATE TYPE charybdis_tests.address (number varchar, street text, city varchar)",
	"create table charybdis_tests.orders (order_id varchar, shipping_address address, primary key(order_id))",
	"create table charybdis_tests.order_items (order_id varchar, item_id varchar, quantity int, primary key((order_id), item_id))",
	"CREATE INDEX order_item_lookup ON charybdis_tests.order_items (item_id)",
	"CREATE MATERIALIZED VIEW charybdis_tests.item_orders AS SELECT * FROM charybdis_tests.order_items WHERE order_id IS NOT NULL AND item_id IS NOT NULL AND (quantity > 0) PRIMARY KEY((item_id), order_id, quantity) WITH CLUSTERING ORDER BY (order_id ASC)",
}

var testClusterConfig utils.ClusterConfigGeneratorFn

// Orders table
var (
	orderColumns = []*metadata.ColumnSpecification{
		{
			Name:              "order_id",
			CQLType:           "varchar",
			IsPartitioningKey: true,
		},
		{
			Name:    "shipping_address",
			CQLType: "address",
		},
	}

	OrdersTableSpec = &metadata.TableSpecification{
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
		CustomTypes: []*metadata.TypeSpecification{
			AddressTypeSpec.Clone(),
		},
	}
)

// Order Items table
var (
	orderItemColumns = []*metadata.ColumnSpecification{
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

	OrderItemsTableSpec = &metadata.TableSpecification{
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

	OrderItemsViewSpec = &metadata.ViewSpecification{
		Name:  "item_orders",
		Table: OrderItemsTableSpec,
		Partitioning: []*metadata.PartitioningColumn{
			{
				Column: orderItemColumns[1],
				Order:  1,
			},
		},
		Clustering: []*metadata.ClusteringColumn{
			{
				Column:     orderItemColumns[0],
				Order:      1,
				Descending: false,
			},
			{
				Column:     orderItemColumns[2],
				Order:      2,
				Descending: false,
			},
		},
		Where: "quantity > 0",
	}
)

// Address type
var (
	addressFields = []*metadata.FieldSpecification{
		{
			Name:    "number",
			CQLType: "varchar",
		},
		{
			Name:    "street",
			CQLType: "text",
		},
		{
			Name:    "city",
			CQLType: "varchar",
		},
	}

	AddressTypeSpec = &metadata.TypeSpecification{
		Name:   "address",
		Fields: slices.Clone(addressFields),
	}
)

type Order struct {
	OrderID         string  `cql:"order_id"`
	ShippingAddress Address `cql:"shipping_address"`
}

type Address struct {
	Number string `cql:"number"`
	Street string `cql:"street"`
	City   string `cql:"city"`
}

type OrderItem struct {
	OrderID  string `cql:"order_id"`
	ItemID   string `cql:"item_id"`
	Quantity int    `cql:"quantity"`
}

func testAddress(number int, street, city string) Address {
	return Address{
		Number: strconv.FormatInt(int64(number), 10),
		Street: street,
		City:   city,
	}
}

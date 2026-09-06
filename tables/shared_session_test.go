package tables_test

import (
	"context"
	"testing"
	"time"

	"github.com/scylladb/gocqlx/v3"
	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/tables"
)

const decoyKeyspace = "charybdis_tests_decoy"

// TestSharedSessionWithoutDefaultKeyspace checks that managers over different tables and views can share
// one session that has no default keyspace, because every statement they issue is keyspace-qualified.
func TestSharedSessionWithoutDefaultKeyspace(t *testing.T) {
	ctx := context.Background()

	cluster := testClusterConfig()
	cluster.Keyspace = ""
	session, errSession := cluster.CreateSession()
	require.NoError(t, errSession)
	defer session.Close()

	items, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithSession(session, 15*time.Second),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err)
	require.Equal(t, TestKeyspace+".order_items", items.QualifiedName())

	orders, err := tables.NewTableManager[Order](ctx,
		tables.WithSession(session, 15*time.Second),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err)
	require.Equal(t, TestKeyspace+".orders", orders.QualifiedName())

	view, err := tables.NewViewManager[OrderItem](ctx,
		tables.WithSession(session, 15*time.Second),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
		tables.WithViewSpecification(OrderItemsViewSpec))
	require.NoError(t, err)
	require.Equal(t, TestKeyspace+".item_orders", view.QualifiedName())

	require.NoError(t, items.Insert(ctx, &OrderItem{OrderID: "shared-session-1", ItemID: "shared-item-1", Quantity: 3}))
	require.NoError(t, items.Upsert(ctx, &OrderItem{OrderID: "shared-session-1", ItemID: "shared-item-2", Quantity: 4}))
	require.NoError(t, orders.Insert(ctx, &Order{OrderID: "shared-session-1", ShippingAddress: testAddress(1, "Shared Street", "Sessionville")}))

	fetched, err := items.GetByPrimaryKey(ctx, "shared-session-1", "shared-item-1")
	require.NoError(t, err)
	require.NotNil(t, fetched)
	require.Equal(t, 3, fetched.Quantity)

	first, err := items.GetByPartitionKey(ctx, "shared-session-1")
	require.NoError(t, err)
	require.NotNil(t, first)

	byIndex, err := items.GetByIndexedColumn(ctx, "item_id", "shared-item-2")
	require.NoError(t, err)
	require.NotNil(t, byIndex)
	require.Equal(t, 4, byIndex.Quantity)

	partitionCount, err := items.CountByPartitionKey(ctx, "shared-session-1")
	require.NoError(t, err)
	require.Equal(t, int64(2), partitionCount)

	total, err := items.Count(ctx)
	require.NoError(t, err)
	require.GreaterOrEqual(t, total, int64(2))

	selected := 0
	require.NoError(t, items.SelectByPartitionKey(ctx, func(ctx context.Context, records []*OrderItem, pageState []byte, newPageState []byte) (bool, error) {
		selected += len(records)
		return true, nil
	}, nil, "shared-session-1"))
	require.Equal(t, 2, selected)

	scanned := 0
	require.NoError(t, items.Scan(ctx, func(ctx context.Context, records []*OrderItem, pageState []byte, newPageState []byte) (bool, error) {
		scanned += len(records)
		return true, nil
	}))
	require.GreaterOrEqual(t, scanned, 2)

	viewRow, err := view.GetByPartitionKey(ctx, "shared-item-1")
	require.NoError(t, err)
	require.NotNil(t, viewRow)
	require.Equal(t, "shared-session-1", viewRow.OrderID)

	viewCount, err := view.CountByPartitionKey(ctx, "shared-item-2")
	require.NoError(t, err)
	require.Equal(t, int64(1), viewCount)

	order, err := orders.GetByPrimaryKey(ctx, "shared-session-1")
	require.NoError(t, err)
	require.NotNil(t, order)
	require.Equal(t, "Shared Street", order.ShippingAddress.Street)

	require.NoError(t, items.Update(ctx, &OrderItem{OrderID: "shared-session-1", ItemID: "shared-item-1", Quantity: 5}))
	require.NoError(t, items.DeleteByPrimaryKey(ctx, "shared-session-1", "shared-item-2"))

	remaining, err := items.CountByPartitionKey(ctx, "shared-session-1")
	require.NoError(t, err)
	require.Equal(t, int64(1), remaining)
}

// TestSharedSessionDefaultKeyspace checks that a session defaulting to the manager's own keyspace is
// accepted, and one defaulting to another keyspace holding a table of the same name is refused.
func TestSharedSessionDefaultKeyspace(t *testing.T) {
	ctx := context.Background()

	admin, errAdmin := gocqlx.WrapSession(testClusterConfig().CreateSession())
	require.NoError(t, errAdmin)
	defer admin.Close()
	require.NoError(t, admin.ExecStmt("CREATE KEYSPACE IF NOT EXISTS "+decoyKeyspace+" WITH replication={'class': 'SimpleStrategy', 'replication_factor': 1}"))
	require.NoError(t, admin.ExecStmt("CREATE TABLE IF NOT EXISTS "+decoyKeyspace+".order_items (order_id varchar, item_id varchar, quantity int, primary key((order_id), item_id))"))
	require.NoError(t, admin.ExecStmt("TRUNCATE "+decoyKeyspace+".order_items"))

	matching := testClusterConfig()
	matching.Keyspace = TestKeyspace
	matchingSession, errMatching := matching.CreateSession()
	require.NoError(t, errMatching)
	defer matchingSession.Close()

	items, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithSession(matchingSession, 15*time.Second),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err)
	require.NoError(t, items.Insert(ctx, &OrderItem{OrderID: "matching-session-1", ItemID: "matching-item-1", Quantity: 7}))

	count, err := items.CountByPartitionKey(ctx, "matching-session-1")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	decoy := testClusterConfig()
	decoy.Keyspace = decoyKeyspace
	decoySession, errDecoy := decoy.CreateSession()
	require.NoError(t, errDecoy)
	defer decoySession.Close()

	_, err = tables.NewTableManager[OrderItem](ctx,
		tables.WithSession(decoySession, 15*time.Second),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.ErrorIs(t, err, tables.ErrSessionKeyspaceMismatch)

	_, err = tables.NewViewManager[OrderItem](ctx,
		tables.WithSession(decoySession, 15*time.Second),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
		tables.WithViewSpecification(OrderItemsViewSpec))
	require.ErrorIs(t, err, tables.ErrSessionKeyspaceMismatch)

	var decoyRows int64
	require.NoError(t, admin.Query("SELECT COUNT(1) FROM "+decoyKeyspace+".order_items", nil).Scan(&decoyRows))
	require.Equal(t, int64(0), decoyRows, "nothing should have been written to the decoy keyspace")
}

func TestWithSessionValidation(t *testing.T) {
	ctx := context.Background()

	_, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithSession(nil, time.Second),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.ErrorIs(t, err, tables.ErrNoSession)

	session, errSession := testClusterConfig().CreateSession()
	require.NoError(t, errSession)
	defer session.Close()

	_, err = tables.NewTableManager[OrderItem](ctx,
		tables.WithSession(session, 0),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.ErrorIs(t, err, tables.ErrNoQueryTimeout)
}

func TestManagersRequireKeyspace(t *testing.T) {
	ctx := context.Background()

	_, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.ErrorIs(t, err, metadata.ErrNoKeyspace)

	_, err = tables.NewViewManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithTableSpecification(OrderItemsTableSpec),
		tables.WithViewSpecification(OrderItemsViewSpec))
	require.ErrorIs(t, err, metadata.ErrNoKeyspace)
}

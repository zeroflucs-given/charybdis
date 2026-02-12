package tables_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/tables"
)

// TestDeleteRecord checks we can delete a record from the table
func TestDeleteRecord(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))

	// Arrange
	require.NoError(t, err, "Should not error starting up")
	obj := &Order{
		OrderID: "delete-test-1",
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	errDelete := manager.Delete(ctx, obj)
	require.NoError(t, errDelete, "Should not error deleting")

	// Assert
	fetched, errGet := manager.GetByPartitionKey(ctx, "delete-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.Nil(t, fetched, "Should yield no result after delete")
}

// TestDeleteByKey checks we can delete by primary key
func TestDeleteByKey(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))

	// Arrange
	require.NoError(t, err, "Should not error starting up")
	obj := &Order{
		OrderID: "delete-test-2",
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	errDelete := manager.DeleteByPrimaryKey(ctx, "delete-test-2")
	require.NoError(t, errDelete, "Should not error deleting")

	// Assert
	fetched, errGet := manager.GetByPartitionKey(ctx, "delete-test-2")
	require.NoError(t, errGet, "Should not error fetching")
	require.Nil(t, fetched, "Should yield no result after delete")
}

// TestDeleteByKey checks we can delete by primary key
func TestDeleteUsingOptions(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](
		ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
	)

	require.NoError(t, err, "Should not error starting up")

	// Test data
	toInsert := []*OrderItem{
		{
			OrderID:  "del-opt-order-01",
			ItemID:   "del-opt-item-02",
			Quantity: 3,
		},
		{
			OrderID:  "del-opt-order-01",
			ItemID:   "del-opt-item-03",
			Quantity: 1,
		},
		{
			OrderID:  "del-opt-order-02",
			ItemID:   "del-opt-item-03",
			Quantity: 2,
		},
		{
			OrderID:  "del-opt-order-01",
			ItemID:   "del-opt-item-01",
			Quantity: 5,
		},
	}

	errInsert := manager.InsertBulk(ctx, toInsert, -1)
	require.NoError(t, errInsert, "Should not error inserting test data")

	errDelete := manager.DeleteUsingOptions(ctx, tables.WithDeletionKey("order_id", "del-opt-order-02"), tables.WithDeletionKey("item_id", "del-opt-item-03"), tables.WithDeleteIfExists())
	require.NoError(t, errDelete, "Should not error deleting")

	// Assert
	fetched, errGet := manager.GetByPartitionKey(ctx, "del-opt-order-02")
	require.NoError(t, errGet, "Should not error fetching")
	require.Nil(t, fetched, "Should yield no result after delete")
}

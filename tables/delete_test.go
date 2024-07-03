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

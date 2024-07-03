package tables_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/tables"
)

// TestTableAndViewManager tests we can insert into a view, then
func TestTableAndViewManager(t *testing.T) {
	// Test globals
	ctx := context.Background()

	// Arrange - Create our table manager, then insert an order item
	tableManager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")
	obj := &OrderItem{
		OrderID:  "insert-test-1",
		ItemID:   "item-test-1",
		Quantity: 1337,
	}
	errInsert := tableManager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act - Create a view manager over the same table, then query it
	viewManager, err := tables.NewViewManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
		tables.WithViewSpecification(OrderItemsViewSpec))
	require.NoError(t, err, "Should not error starting up")

	fetched, errGet := viewManager.GetByPartitionKey(ctx, "item-test-1")

	// Assert
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, 1337, fetched.Quantity, "Should have non-key fields set")
}

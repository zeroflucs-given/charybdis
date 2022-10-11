package tables_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zeroflucs-given/charybdis/tables"
)

// TestUpdateRecord checks we can update an existing record
func TestUpdateRecord(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "update-test-1",
		ShippingAddress: "Some address",
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	updated := &Order{
		OrderID:         "update-test-1",
		ShippingAddress: "new address",
	}
	errUpdate := manager.Update(ctx, updated)

	// Assert
	require.NoError(t, errUpdate, "No error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "update-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, "new address", fetched.ShippingAddress, "Change should have persisted")
}

// TestUpdateRecordWithTTL checks we can update an existing record with a TTL
// then the record disappears on queue
func TestUpdateRecordWithTTL(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "update-test-2",
		ShippingAddress: "Some address",
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	updated := &Order{
		OrderID:         "update-test-2",
		ShippingAddress: "new address",
	}
	errUpdate := manager.Update(ctx, updated, tables.WithTTL(time.Second))
	time.Sleep(time.Second * 2)

	// Assert
	require.NoError(t, errUpdate, "No error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "update-test-2")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get an object back")
	require.Equal(t, "", fetched.ShippingAddress, "Shipping address should now disappear")
}

// TestUpdateRecordNotExist checks updates fail when a record does not exist
func TestUpdateRecordNotExist(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	// <crickets>

	// Act
	updated := &Order{
		OrderID:         "update-test-3",
		ShippingAddress: "new address",
	}
	errUpdate := manager.Update(ctx, updated)

	// Assert
	require.ErrorIs(t, errUpdate, tables.ErrPreconditionFailed, "Should fail the precondition")
}

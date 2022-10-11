package tables_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeroflucs-given/charybdis/tables"
)

// TestUpsertRecords checks we can upsert an existing record
func TestUpsertRecords(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "upsert-test-1",
		ShippingAddress: "Some address",
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	updated := &Order{
		OrderID:         "upsert-test-1",
		ShippingAddress: "new address",
	}
	errUpdate := manager.Upsert(ctx, updated)

	// Assert
	require.NoError(t, errUpdate, "No error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "upsert-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, "new address", fetched.ShippingAddress, "Change should have persisted")
}

// TestUpsertRecordNotExist checks upserts dont fail when a record does not exist
func TestUpsertRecordNotExist(t *testing.T) {
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
		OrderID:         "upsert-test-1",
		ShippingAddress: "new address",
	}
	errUpdate := manager.Upsert(ctx, updated)

	// Assert
	require.NoError(t, errUpdate, "No error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "upsert-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, "new address", fetched.ShippingAddress, "Change should have persisted")
}

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
		ShippingAddress: testAddress(1, "Insert Street", "Somerville"),
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	updated := &Order{
		OrderID:         "update-test-1",
		ShippingAddress: testAddress(2, "Update Street", "Somerville"),
	}
	errUpdate := manager.Update(ctx, updated)

	// Assert
	require.NoError(t, errUpdate, "No error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "update-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, testAddress(2, "Update Street", "Somerville"), fetched.ShippingAddress, "Change should have persisted")
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
		ShippingAddress: testAddress(1, "Insert Street", "Somerville"),
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	updated := &Order{
		OrderID:         "update-test-2",
		ShippingAddress: testAddress(2, "Update Street", "Somerville"),
	}
	errUpdate := manager.Update(ctx, updated, tables.WithTTL(time.Second))
	time.Sleep(time.Second * 2)

	// Assert
	require.NoError(t, errUpdate, "No error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "update-test-2")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get an object back")
	require.Equal(t, Address{}, fetched.ShippingAddress, "Shipping address should now disappear")
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
		ShippingAddress: testAddress(3, "Update Street", "Somerville"),
	}
	errUpdate := manager.Update(ctx, updated)

	// Assert
	require.ErrorIs(t, errUpdate, tables.ErrPreconditionFailed, "Should fail the precondition")
}

// TestWithSimpleIf checks upserts succeed if predicate satisfied
func TestWithSimpleIf(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Upsert initial
	orig := &Order{
		OrderID:         "update-test-4",
		ShippingAddress: testAddress(4, "Original Street", "Somerville"),
	}
	errInsert := manager.Insert(ctx, orig)
	require.NoError(t, errInsert, "No error inserting")

	// Test
	updated := &Order{
		OrderID:         "update-test-4",
		ShippingAddress: testAddress(4, "Update Street", "Somerville"),
	}
	option := tables.WithSimpleIf("shipping_address", testAddress(4, "Original Street", "Somerville"))
	errUpdate := manager.Update(ctx, updated, option)

	// Assert
	require.NoError(t, errUpdate, "Expect no error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "update-test-4")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, testAddress(4, "Update Street", "Somerville"), fetched.ShippingAddress, "Change should have persisted")
}

// TestWithSimpleIfWrongValue checks upserts succeed if predicate satisfied
func TestWithSimpleIfWrongValue(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Upsert initial
	orig := &Order{
		OrderID:         "update-test-5",
		ShippingAddress: testAddress(4, "Start Street", "Somerville"),
	}
	errInsert := manager.Insert(ctx, orig)
	require.NoError(t, errInsert, "No error inserting")

	// Test
	updated := &Order{
		OrderID:         "update-test-5",
		ShippingAddress: testAddress(5, "Update Street", "Somerville"),
	}
	option := tables.WithSimpleIf("shipping_address", "address")
	errUpdate := manager.Update(ctx, updated, option)

	// Assert
	require.Error(t, errUpdate, "Expect error updating")
}

// TestWithSimpleIfNotExists checks upserts succeed if predicate satisfied
func TestWithSimpleIfNotExists(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Test
	updated := &Order{
		OrderID:         "update-test-6",
		ShippingAddress: testAddress(6, "Update Street", "Somerville"),
	}
	option := tables.WithSimpleIf("shipping_address", "address")
	errUpdate := manager.Update(ctx, updated, option)

	// Assert
	require.Error(t, errUpdate, "Expect error updating")
}

package tables_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/zeroflucs-given/charybdis/tables"
)

// TestUpsertRecords checks we can upsert an existing record
func TestUpsertRecords(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec),
		tables.WithLogger(zaptest.NewLogger(t)),
	)
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "upsert-test-1",
		ShippingAddress: testAddress(1, "Initial Street", "Somerville"),
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	updated := &Order{
		OrderID:         "upsert-test-1",
		ShippingAddress: testAddress(1, "Upsert Street", "Somerville"),
	}

	errUpdate := manager.Upsert(ctx, updated, tables.WithUpsertExists())
	require.NoError(t, errUpdate, "No error updating")

	fetched, errGet := manager.GetByPartitionKey(ctx, "upsert-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, testAddress(1, "Upsert Street", "Somerville"), fetched.ShippingAddress, "Change should have persisted")
}

// TestUpsertRecordNotExist checks upserts don't fail when a record does not exist
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
		OrderID:         "upsert-test-2",
		ShippingAddress: testAddress(2, "Upsert Street", "Somerville"),
	}
	errUpdate := manager.Upsert(ctx, updated)

	// Assert
	require.NoError(t, errUpdate, "No error updating")
	fetched, errGet := manager.GetByPartitionKey(ctx, "upsert-test-2")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, testAddress(2, "Upsert Street", "Somerville"), fetched.ShippingAddress, "Change should have persisted")
}

// TestWithSimpleIfNotFound checks that no update happens if the existing record isn't found
func TestWithSimpleIfNotFound(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Upsert initial
	orig := &Order{
		OrderID:         "upsert-test-3",
		ShippingAddress: testAddress(3, "Initial Street", "Somerville"),
	}
	errInsert := manager.Insert(ctx, orig)
	require.NoError(t, errInsert, "No error inserting")

	// Test
	updated := &Order{
		OrderID:         "upsert-test-3",
		ShippingAddress: testAddress(3, "Upsert Street", "Somerville"),
	}
	option := tables.WithSimpleUpsertIf("shipping_address", testAddress(3, "Different Street", "Somerville"))
	errUpdate := manager.Upsert(ctx, updated, option)

	// Assert
	require.NoError(t, errUpdate, "Expect no error upserting")
	fetched, errGet := manager.GetByPartitionKey(ctx, "upsert-test-3")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, testAddress(3, "Initial Street", "Somerville"), fetched.ShippingAddress, "Should be no change")
}

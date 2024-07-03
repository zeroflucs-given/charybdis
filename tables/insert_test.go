package tables_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/tables"
)

// TestInsertRecord checks we can insert a record and get it back
func TestInsertRecord(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "insert-test-1",
		ShippingAddress: "Some address",
	}

	// Act
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Assert
	fetched, errGet := manager.GetByPartitionKey(ctx, "insert-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, "Some address", fetched.ShippingAddress, "Should have non-key fields set")
}

// TestInsertRecordBuild performs bulk insert testing
func TestInsertRecordBulk(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	orders := make([]*Order, 5)
	for i := 0; i < len(orders); i++ {
		orders[i] = &Order{
			OrderID:         fmt.Sprintf("bulk-order-%d", i),
			ShippingAddress: fmt.Sprintf("Shipping address for %d", i),
		}
	}

	// Act
	errBulk := manager.InsertBulk(ctx, orders, -1)

	// Assert
	require.NoError(t, errBulk, "Should not error bulk inserting")
	fetched, errFetch := manager.GetByPartitionKey(ctx, "bulk-order-1")
	require.NoError(t, errFetch, "Should not fail re-fetching")
	require.NotNil(t, fetched, "Should have an object")
	require.Equal(t, "Shipping address for 1", fetched.ShippingAddress, "Should have correct state")
}

// TestInsertDuplicates checks that a duplicated insert fails with the expected error
func TestInsertDuplicates(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "insert-test-dupe",
		ShippingAddress: "Some address",
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	dupe := &Order{
		OrderID:         "insert-test-dupe",
		ShippingAddress: "another address",
	}
	errInsertDupe := manager.Insert(ctx, dupe)
	require.ErrorIs(t, errInsertDupe, tables.ErrPreconditionFailed, "Should get a precondition failure")

	// Assert
	fetched, errGet := manager.GetByPartitionKey(ctx, "insert-test-1")
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, "Some address", fetched.ShippingAddress, "Should have original state")
}

// TestInsertRecordWithTTL checks we can insert a record and that it's not there after a delay
func TestInsertRecordWithTTL(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "insert-test-ttl",
		ShippingAddress: "Some address",
	}

	// Act
	errInsert := manager.Insert(ctx, obj, tables.WithTTL(time.Second))
	require.NoError(t, errInsert, "Should not error inserting")
	time.Sleep(time.Second * 2)

	// Assert
	fetched, errGet := manager.GetByPartitionKey(ctx, "insert-test-tll")
	require.NoError(t, errGet, "Should not error fetching")
	require.Nil(t, fetched, "Should get no object back")
}

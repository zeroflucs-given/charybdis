package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zeroflucs-given/charybdis/tables"
)

// TestGetByKeys makes sure that we can get by primary and partition key with expected behaviour
func TestGetByKeys(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	errInsert := manager.InsertBulk(ctx, []*OrderItem{
		{
			OrderID:  "pk-order-1",
			ItemID:   "pk-item-3",
			Quantity: 1,
		},
		{
			OrderID:  "pk-order-1",
			ItemID:   "pk-item-2",
			Quantity: 2,
		},
		{
			OrderID:  "pk-order-1",
			ItemID:   "pk-item-1",
			Quantity: 3,
		},
	}, -1)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	byPK, errPK := manager.GetByPrimaryKey(ctx, "pk-order-1", "pk-item-2")
	byPart, errPart := manager.GetByPartitionKey(ctx, "pk-order-1")

	// Assert
	require.NoError(t, errPK, "Should not error on GetByPrimaryKey")
	require.NotNil(t, byPK, "Should get result by primary key")
	require.Equal(t, "pk-item-2", byPK.ItemID, "Should respect cluster search predicate")
	require.NoError(t, errPart, "Should not error on GetByPartitionKey")
	require.NotNil(t, byPart, "Should get result by partition key")
	require.Equal(t, "pk-item-1", byPart.ItemID, "Should respect clustering order")
}

// TestIndexedGet checks we can get a record back from an index
func TestIndexedGet(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &OrderItem{
		OrderID:  "index-order-1",
		ItemID:   "index-item-1",
		Quantity: 3,
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Assert
	fetched, errGet := manager.GetByIndexedColumn(ctx, "item_id", "index-item-1")

	// Assert
	require.NoError(t, errGet, "Should not error fetching")
	require.NotNil(t, fetched, "Should get object back")
	require.Equal(t, 3, fetched.Quantity, "Should have non-key fields set")
}

// TestIndexedGetNotExist checks we get nothing back when an index lookup fails
func TestIndexedGetNotExist(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	// <crickets>

	// Assert
	fetched, errGet := manager.GetByIndexedColumn(ctx, "item_id", "index-item-not-existing")

	// Assert
	require.NoError(t, errGet, "Should not error fetching")
	require.Nil(t, fetched, "Should get no object back")
}

// TestSelectByPartitionKey performs a baisc seelect by partition keys in order to
// determine that we can page through the data.
func TestSelectByPartitionKey(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	orders := 500
	itemsPerOrder := 10
	toInsert := make([]*OrderItem, 0, orders*itemsPerOrder)
	for order := 0; order < orders; order++ {
		for item := 0; item < itemsPerOrder; item++ {
			toInsert = append(toInsert, &OrderItem{
				OrderID:  fmt.Sprintf("sp-order-%d", order),
				ItemID:   fmt.Sprintf("sp-item-%d", item),
				Quantity: (order * item) % 27,
			})
		}
	}
	errInsert := manager.InsertBulk(ctx, toInsert, -1)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	recordCount := 0
	expectOrder := "sp-order-257"
	errSelect := manager.SelectByPartitionKey(ctx, func(ctx context.Context, records []*OrderItem, pageState []byte, newPageState []byte) (bool, error) {
		recordCount += len(records)
		for _, rec := range records {
			if rec.OrderID != expectOrder {
				return false, fmt.Errorf("wrong order ID: %v", rec.OrderID)
			}
		}
		return true, nil
	}, nil, expectOrder)

	// Assert
	require.NoError(t, errSelect, "Should not error selecting")
	require.Equal(t, 10, recordCount, "Should have right number of records.")
}

// TestSelectByIndexedColumn checks we can get values by their indexed column
func TestSelectByIndexedColumn(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	orders := 500
	itemsPerOrder := 10
	toInsert := make([]*OrderItem, 0, orders*itemsPerOrder)
	for order := 0; order < orders; order++ {
		for item := 0; item < itemsPerOrder; item++ {
			toInsert = append(toInsert, &OrderItem{
				OrderID:  fmt.Sprintf("ix-order-%d", order),
				ItemID:   fmt.Sprintf("ix-item-%d", item),
				Quantity: (order * item) % 27,
			})
		}
	}
	errInsert := manager.InsertBulk(ctx, toInsert, -1)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	recordCount := 0
	expectItem := "ix-item-5"
	errSelect := manager.SelectByIndexedColumn(ctx, func(ctx context.Context, records []*OrderItem, pageState []byte, newPageState []byte) (bool, error) {
		recordCount += len(records)
		for _, rec := range records {
			if rec.ItemID != expectItem {
				return false, fmt.Errorf("wrong item ID: %v", rec.ItemID)
			}
		}
		return true, nil
	}, "item_id", "ix-item-5")

	// Assert
	require.NoError(t, errSelect, "Should not error selecting")
	require.Equal(t, 900, recordCount, "Should have right number of records.")
}

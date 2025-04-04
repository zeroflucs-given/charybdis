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

func TestSelectByPrimaryKey(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	orders := 10
	itemsPerOrder := 10
	toInsert := make([]*OrderItem, 0, orders*itemsPerOrder)
	for order := 0; order < orders; order++ {
		for item := 0; item < itemsPerOrder; item++ {
			toInsert = append(toInsert, &OrderItem{
				OrderID:  fmt.Sprintf("sprim-order-%d", order),
				ItemID:   fmt.Sprintf("sprim-item-%d", item),
				Quantity: (order * item) % 27,
			})
		}
	}
	errInsert := manager.InsertBulk(ctx, toInsert, -1)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	recordCount := 0
	expectOrder := "sprim-order-1"
	expectItem := "sprim-item-1"
	errSelect := manager.SelectByPrimaryKey(ctx, func(ctx context.Context, records []*OrderItem, pageState []byte, newPageState []byte) (bool, error) {
		recordCount += len(records)
		for _, rec := range records {
			if rec.OrderID != expectOrder {
				return false, fmt.Errorf("wrong order ID: %v", rec.OrderID)
			}
			if rec.ItemID != expectItem {
				return false, fmt.Errorf("wrong item ID: %v", rec.ItemID)
			}
		}
		return true, nil
	}, nil, expectOrder, expectItem)

	// Assert
	require.NoError(t, errSelect, "Should not error selecting")
	require.Equal(t, 1, recordCount, "Should have right number of records.")
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

// TestSelectWithSortOrder checks we can get values sorted by their indexed columns
func TestSelectWithSortOrder(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	toInsert := []*OrderItem{
		{
			OrderID:  "sort-order-01",
			ItemID:   "sort-item-02",
			Quantity: 3,
		},
		{
			OrderID:  "sort-order-01",
			ItemID:   "sort-item-03",
			Quantity: 1,
		},
		{
			OrderID:  "sort-order-02",
			ItemID:   "sort-item-03",
			Quantity: 2,
		},
		{
			OrderID:  "sort-order-01",
			ItemID:   "sort-item-01",
			Quantity: 5,
		},
	}

	errInsert := manager.InsertBulk(ctx, toInsert, -1)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	recordCount := 0
	lastItem := ""
	errSelect := manager.SelectByPartitionKey(ctx, func(ctx context.Context, records []*OrderItem, pageState []byte, newPageState []byte) (bool, error) {
		recordCount += len(records)
		for _, rec := range records {
			if rec.ItemID < lastItem {
				return false, fmt.Errorf("quantity out of order: %s -> %s", lastItem, rec.ItemID)
			}
			lastItem = rec.ItemID
		}
		return true, nil
	}, []tables.QueryOption{tables.WithSort("item_id", 1)}, "sort-order-01")

	// Assert
	require.NoError(t, errSelect, "Should not error selecting")
	require.Equal(t, 3, recordCount, "Should have correct number of records.")
}

// TestSelectWithSortOrder checks we can get values sorted by their indexed columns
func TestSelectWithColumn(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	toInsert := []*OrderItem{
		{
			OrderID:  "col-order-01",
			ItemID:   "col-item-02",
			Quantity: 3,
		},
		{
			OrderID:  "col-order-01",
			ItemID:   "col-item-03",
			Quantity: 1,
		},
		{
			OrderID:  "col-order-02",
			ItemID:   "col-item-03",
			Quantity: 2,
		},
		{
			OrderID:  "col-order-01",
			ItemID:   "col-item-01",
			Quantity: 5,
		},
	}

	errInsert := manager.InsertBulk(ctx, toInsert, -1)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	recordCount := 0
	errSelect := manager.SelectByPartitionKey(ctx, func(ctx context.Context, records []*OrderItem, pageState []byte, newPageState []byte) (bool, error) {
		recordCount += len(records)
		for _, rec := range records {
			if rec.Quantity != 0 {
				return false, fmt.Errorf("expected quantity not zero: %d", rec.Quantity)
			}
		}
		return true, nil
	}, []tables.QueryOption{tables.WithColumns("item_id")}, "col-order-01")

	// Assert
	require.NoError(t, errSelect, "Should not error selecting")
	require.Equal(t, 3, recordCount, "Should have correct number of records.")
}

// TestSelectWithSortOrder checks we can get values sorted by their indexed columns
func TestGetUsingOptions(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](
		ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
	)
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	toInsert := []*OrderItem{
		{
			OrderID:  "opt-order-01",
			ItemID:   "opt-item-02",
			Quantity: 3,
		},
		{
			OrderID:  "opt-order-01",
			ItemID:   "opt-item-03",
			Quantity: 1,
		},
		{
			OrderID:  "opt-order-02",
			ItemID:   "opt-item-03",
			Quantity: 2,
		},
		{
			OrderID:  "opt-order-01",
			ItemID:   "opt-item-01",
			Quantity: 5,
		},
	}

	errInsert := manager.InsertBulk(ctx, toInsert, -1)
	require.NoError(t, errInsert, "Should not error inserting test data")

	// tests

	res, err := manager.GetUsingOptions(ctx, tables.WithKey("order_id", "opt-order-01"), tables.WithKey("item_id", "opt-item-03"))
	require.NoError(t, err, "Should not error getting row")
	require.Equal(t, 1, res.Quantity, "expected quantity to be 1")

	res, err = manager.GetUsingOptions(ctx, tables.WithKey("order_id", "opt-order-01"))
	require.NoError(t, err, "Should not error getting row")
	require.Equal(t, 5, res.Quantity, "expected quantity to be 5") // oughta be the latest value inserted with that key

	// Same as the first test, just specified with alternate options
	res, err = manager.GetUsingOptions(ctx, tables.WithColumnsEqual("order_id", "item_id"), tables.WithBindings("opt-order-01", "opt-item-03"))
	require.NoError(t, err, "Should not error getting row")
	require.Equal(t, 1, res.Quantity, "expected quantity to be 1")

}

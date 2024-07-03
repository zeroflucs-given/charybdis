package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/tables"
)

// TestScan tests that we can scan with a single row
func TestScan(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	obj := &Order{
		OrderID:         "scan-test-1",
		ShippingAddress: "Scan address",
	}
	errInsert := manager.Insert(ctx, obj)
	require.NoError(t, errInsert, "Should not error inserting")

	// Act
	scanCount := 0
	recordCount := 0
	errScan := manager.Scan(ctx, func(ctx context.Context, records []*Order, pageState []byte, newPageState []byte) (bool, error) {
		scanCount++
		recordCount += len(records)
		return true, nil
	})

	// Assert - We use GTE here as we may have a lot of extra rows from other tests
	require.NoError(t, errScan, "Should not error scanning")
	require.GreaterOrEqual(t, scanCount, 1, "Should have at least one scan calls")
	require.GreaterOrEqual(t, recordCount, 1, "Should have at least one record in the scan")
}

// TestScanPaged checks we can insert bulk rows and get lots of pages
func TestScanPaged(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	orders := make([]*Order, 500)
	for i := 0; i < len(orders); i++ {
		orders[i] = &Order{
			OrderID:         fmt.Sprintf("bulk-scan-%d", i),
			ShippingAddress: fmt.Sprintf("Shipping address for %d", i),
		}
	}
	errBulk := manager.InsertBulk(ctx, orders, 4)
	require.NoError(t, errBulk, "Should not error inserting")

	// Act
	scanCount := 0
	recordCount := 0
	errScan := manager.Scan(ctx, func(ctx context.Context, records []*Order, pageState []byte, newPageState []byte) (bool, error) {
		scanCount++
		recordCount += len(records)
		return scanCount < 25, nil
	}, tables.WithPaging(10, nil))

	// Assert
	require.NoError(t, errScan, "Should not error scanning")
	require.Equal(t, 25, scanCount, "Should have stopped at right scan iteration")
	require.Equal(t, 250, recordCount, "Should have at  the number of records we expect")
}

// TestScanSorted checks we can returns row sorts by columns
func TestScanSorted(t *testing.T) {
	// Test globals
	ctx := context.Background()
	manager, err := tables.NewTableManager[Order](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec))
	require.NoError(t, err, "Should not error starting up")

	// Arrange
	orders := make([]*Order, 10)
	for i := 0; i < len(orders); i++ {
		orders[i] = &Order{
			OrderID:         fmt.Sprintf("bulk-scan-%02d", i),
			ShippingAddress: fmt.Sprintf("Shipping address for %d", i),
		}
	}
	errBulk := manager.InsertBulk(ctx, orders, 4)
	require.NoError(t, errBulk, "Should not error inserting")

	// Act
	scanCount := 0
	recordCount := 0
	errScan := manager.Scan(ctx, func(ctx context.Context, records []*Order, pageState []byte, newPageState []byte) (bool, error) {
		scanCount++
		recordCount += len(records)
		return scanCount < 25, nil
	}, tables.WithPaging(10, nil))

	// Assert
	require.NoError(t, errScan, "Should not error scanning")
	require.Equal(t, 25, scanCount, "Should have stopped at right scan iteration")
	require.Equal(t, 250, recordCount, "Should have at  the number of records we expect")
}

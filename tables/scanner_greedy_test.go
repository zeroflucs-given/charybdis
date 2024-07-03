package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/tables"
)

// TestScanGreedy tests the greedy scanner
func TestScanGreedy(t *testing.T) {
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
			OrderID:         fmt.Sprintf("greedy-scan-%d", i),
			ShippingAddress: fmt.Sprintf("Shipping address for %d", i),
		}
	}
	errBulk := manager.InsertBulk(ctx, orders, 4)
	require.NoError(t, errBulk, "Should not error inserting")

	// Act
	var scanner tables.GreedyScanner[Order]
	errScan := manager.Scan(ctx, scanner.OnPage, tables.WithPaging(10, nil))

	// Assert
	require.NoError(t, errScan, "Should not error scanning")
	require.GreaterOrEqual(t, len(scanner.Result()), 250, "Should have at least the number of records we expect")
}

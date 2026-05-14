package tables_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/zeroflucs-given/charybdis/generator"
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

func TestRolesAndGrants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		roleName = "testing_role_create"
	)

	cluster := testClusterConfig()
	logger := zaptest.NewLogger(t)

	session, errSession := cluster.CreateSession()
	require.NoError(t, errSession)

	gen := generator.NewDefinitionGenerator(logger, session)

	var err error

	err = gen.CreateRole(ctx, roleName)
	require.NoError(t, err)

	err = gen.UpdateRole(ctx, roleName, generator.WithRolePassword("foo"))
	require.NoError(t, err)
}

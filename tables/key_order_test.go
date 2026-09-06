package tables_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/metadata"
	"github.com/zeroflucs-given/charybdis/tables"
)

// KeyOrderRow declares its clustering column before its partition columns, which must not change
// the positional binding order (partition keys, then clustering keys) of the key lookups.
type KeyOrderRow struct {
	ID    int64  `cql:"id"`
	Owner string `cql:"owner"`
	Kind  string `cql:"kind"`
}

func TestPrimaryKeyBindingOrderIgnoresColumnDeclarationOrder(t *testing.T) {
	ctx := context.Background()
	cols := []*metadata.ColumnSpecification{
		{Name: "id", CQLType: "bigint", IsClusteringKey: true},
		{Name: "owner", CQLType: "varchar", IsPartitioningKey: true},
		{Name: "kind", CQLType: "varchar", IsPartitioningKey: true},
	}
	spec := &metadata.TableSpecification{
		Name:    "key_order",
		Columns: cols,
		Partitioning: []*metadata.PartitioningColumn{
			{Column: cols[1], Order: 1},
			{Column: cols[2], Order: 2},
		},
		Clustering: []*metadata.ClusteringColumn{{Column: cols[0], Order: 1}},
	}
	manager, err := tables.NewTableManager[KeyOrderRow](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(spec))
	require.NoError(t, err)

	require.NoError(t, manager.Insert(ctx, &KeyOrderRow{ID: 42, Owner: "owner-a", Kind: "kind-a"}))

	row, err := manager.GetByPrimaryKey(ctx, "owner-a", "kind-a", int64(42))
	require.NoError(t, err)
	require.NotNil(t, row)
	require.Equal(t, int64(42), row.ID)

	count, err := manager.CountByPartitionKey(ctx, "owner-a", "kind-a")
	require.NoError(t, err)
	require.Equal(t, int64(1), count)

	require.NoError(t, manager.DeleteByPrimaryKey(ctx, "owner-a", "kind-a", int64(42)))
}

package metadata

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQualifiedName(t *testing.T) {
	name, err := QualifiedName("shop", "orders")
	require.NoError(t, err)
	require.Equal(t, "shop.orders", name)

	_, err = QualifiedName("", "orders")
	require.ErrorIs(t, err, ErrNoKeyspace)

	_, err = QualifiedName("  ", "orders")
	require.ErrorIs(t, err, ErrNoKeyspace)
}

func TestToCQLX(t *testing.T) {
	col := &ColumnSpecification{Name: "id", CQLType: "varchar", IsPartitioningKey: true}
	spec := &TableSpecification{
		Name:         "orders",
		Columns:      []*ColumnSpecification{col},
		Partitioning: []*PartitioningColumn{{Column: col, Order: 1}},
	}
	view := &ViewSpecification{
		Name:         "orders_by_id",
		Table:        spec,
		Partitioning: []*PartitioningColumn{{Column: col, Order: 1}},
	}

	table, err := spec.ToCQLX("shop")
	require.NoError(t, err)
	require.Equal(t, "shop.orders", table.Name())

	viewTable, err := view.ToCQLX("shop")
	require.NoError(t, err)
	require.Equal(t, "shop.orders_by_id", viewTable.Name())

	_, err = spec.ToCQLX("")
	require.ErrorIs(t, err, ErrNoKeyspace)

	_, err = view.ToCQLX("")
	require.ErrorIs(t, err, ErrNoKeyspace)

	var nilSpec *TableSpecification
	_, err = nilSpec.ToCQLX("shop")
	require.ErrorIs(t, err, ErrNoObject)
}

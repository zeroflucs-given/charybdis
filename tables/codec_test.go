package tables_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/tables"
)

// orderItemCodec is the hand-written equivalent of a crudli-generated row codec
type orderItemCodec struct{}

func (orderItemCodec) Columns() []string {
	return []string{"order_id", "item_id", "quantity"}
}

func (orderItemCodec) ScanNext(iter *gocql.Iter, scratch []any) (*OrderItem, bool) {
	v := &OrderItem{}
	scratch[0] = &v.OrderID
	scratch[1] = &v.ItemID
	scratch[2] = &v.Quantity
	if !iter.Scan(scratch...) {
		return nil, false
	}
	return v, true
}

func (orderItemCodec) BindColumns(v *OrderItem, names []string) []any {
	out := make([]any, len(names))
	for i, n := range names {
		switch n {
		case "order_id":
			out[i] = v.OrderID
		case "item_id":
			out[i] = v.ItemID
		case "quantity":
			out[i] = v.Quantity
		}
	}
	return out
}

func codecManager(t testing.TB) tables.TableManager[OrderItem] {
	manager, err := tables.NewTableManager[OrderItem](context.Background(),
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
		tables.WithRowCodec[OrderItem](orderItemCodec{}))
	if err != nil {
		t.Fatalf("creating codec table manager: %v", err)
	}
	return manager
}

// TestCodecRoundTrip covers the codec fast paths end to end against the reflection paths
func TestCodecRoundTrip(t *testing.T) {
	ctx := context.Background()
	manager := codecManager(t)
	plain, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err)

	obj := &OrderItem{OrderID: "codec-order-1", ItemID: "codec-item-1", Quantity: 7}
	require.NoError(t, manager.Upsert(ctx, obj), "Codec upsert should succeed")

	// Codec-written row must read back identically through both mapping paths
	viaCodec, err := manager.GetByPrimaryKey(ctx, "codec-order-1", "codec-item-1")
	require.NoError(t, err)
	viaReflect, err := plain.GetByPrimaryKey(ctx, "codec-order-1", "codec-item-1")
	require.NoError(t, err)
	require.Equal(t, obj, viaCodec)
	require.Equal(t, viaReflect, viaCodec)

	byPartition, err := manager.GetByPartitionKey(ctx, "codec-order-1")
	require.NoError(t, err)
	require.Equal(t, obj, byPartition)

	obj.Quantity = 9
	require.NoError(t, manager.Update(ctx, obj), "Codec update should succeed")
	updated, err := manager.GetByPrimaryKey(ctx, "codec-order-1", "codec-item-1")
	require.NoError(t, err)
	require.Equal(t, 9, updated.Quantity)

	// Reflection-written row must read back through the codec
	other := &OrderItem{OrderID: "codec-order-1", ItemID: "codec-item-2", Quantity: 3}
	require.NoError(t, plain.Insert(ctx, other))
	fetched, err := manager.GetByPrimaryKey(ctx, "codec-order-1", "codec-item-2")
	require.NoError(t, err)
	require.Equal(t, other, fetched)

	// Missing rows still surface as a silent nil
	missing, err := manager.GetByPrimaryKey(ctx, "codec-order-1", "codec-item-none")
	require.NoError(t, err)
	require.Nil(t, missing)
}

// TestCodecPagedSelects checks codec scanning across pages, including pagination-only options
func TestCodecPagedSelects(t *testing.T) {
	const numRows = 250

	ctx := context.Background()
	manager := codecManager(t)

	seed := make([]*OrderItem, numRows)
	for i := range seed {
		seed[i] = &OrderItem{OrderID: "codec-paging-order", ItemID: fmt.Sprintf("codec-page-%03d", i), Quantity: i}
	}
	require.NoError(t, manager.UpsertBulk(ctx, seed, 0))

	seen := map[string]int{}
	pages := 0
	opts := []tables.QueryOption{tables.WithPaging(100, nil)}
	err := manager.SelectByPartitionKey(ctx, func(ctx context.Context, records []*OrderItem, prev []byte, next []byte) (bool, error) {
		pages++
		for _, r := range records {
			seen[r.ItemID]++
		}
		return true, nil
	}, opts, "codec-paging-order")
	require.NoError(t, err)
	require.Len(t, seen, numRows, "Should see every row")
	require.GreaterOrEqual(t, pages, 3, "Should deliver across multiple pages")
	for id, count := range seen {
		require.Equalf(t, 1, count, "Row %s should be delivered exactly once", id)
	}
}

// TestCodecColumnProjectionFallsBack checks WithColumns still works by reverting to reflection
func TestCodecColumnProjectionFallsBack(t *testing.T) {
	ctx := context.Background()
	manager := codecManager(t)

	obj := &OrderItem{OrderID: "codec-proj-order", ItemID: "codec-proj-item", Quantity: 5}
	require.NoError(t, manager.Upsert(ctx, obj))

	var rows []*OrderItem
	err := manager.SelectByPartitionKey(ctx, func(ctx context.Context, records []*OrderItem, prev []byte, next []byte) (bool, error) {
		rows = append(rows, records...)
		return true, nil
	}, []tables.QueryOption{tables.WithColumns("order_id", "item_id")}, "codec-proj-order")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "codec-proj-item", rows[0].ItemID)
	require.Zero(t, rows[0].Quantity, "Projected-out column should stay zero")
}

// TestCodecNullColumn checks a null non-key column scans to the zero value
func TestCodecNullColumn(t *testing.T) {
	ctx := context.Background()
	manager := codecManager(t)

	sess, err := gocqlx.WrapSession(gocql.NewSession(*testClusterConfig()))
	require.NoError(t, err)
	defer sess.Close()
	require.NoError(t, sess.ExecStmt("INSERT INTO charybdis_tests.order_items (order_id, item_id) VALUES ('codec-null-order', 'codec-null-item')"))

	fetched, err := manager.GetByPrimaryKey(ctx, "codec-null-order", "codec-null-item")
	require.NoError(t, err)
	require.NotNil(t, fetched)
	require.Zero(t, fetched.Quantity)
}

// mismatchedCodec declares a column set that disagrees with the table specification
type mismatchedCodec struct{ orderItemCodec }

func (mismatchedCodec) Columns() []string { return []string{"order_id", "quantity"} }

// TestCodecMismatchRejected checks a codec generated against a different column set fails startup
func TestCodecMismatchRejected(t *testing.T) {
	_, err := tables.NewTableManager[OrderItem](context.Background(),
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
		tables.WithRowCodec[OrderItem](mismatchedCodec{}))
	require.Error(t, err, "Mismatched codec should fail construction")
}

// brokenOrderCodec would lose every row if it were ever used
type brokenOrderCodec struct{}

func (brokenOrderCodec) Columns() []string { return []string{"order_id", "shipping_address"} }
func (brokenOrderCodec) ScanNext(iter *gocql.Iter, scratch []any) (*Order, bool) {
	return nil, false
}
func (brokenOrderCodec) BindColumns(v *Order, names []string) []any {
	return make([]any, len(names))
}

// TestCodecUDTTableRejected checks a codec supplied for a table with custom types fails startup
func TestCodecUDTTableRejected(t *testing.T) {
	_, err := tables.NewTableManager[Order](context.Background(),
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec),
		tables.WithRowCodec[Order](brokenOrderCodec{}))
	require.Error(t, err, "Codec on a table with custom types should fail construction")
}

// TestCodecWrongTypeRejected checks a codec for a different row type fails construction loudly
func TestCodecWrongTypeRejected(t *testing.T) {
	_, err := tables.NewTableManager[Order](context.Background(),
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec),
		tables.WithRowCodec[OrderItem](orderItemCodec{}))
	require.Error(t, err, "Codec for the wrong row type should be rejected")
}

func BenchmarkGetByPrimaryKeyCodec(b *testing.B) {
	ctx := context.Background()
	manager := codecManager(b)

	seed := &OrderItem{OrderID: "bench-codec-order", ItemID: "bench-codec-item", Quantity: 3}
	if err := manager.Upsert(ctx, seed); err != nil {
		b.Fatalf("seeding: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.GetByPrimaryKey(ctx, "bench-codec-order", "bench-codec-item"); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkUpsertCodec(b *testing.B) {
	ctx := context.Background()
	manager := codecManager(b)

	obj := &OrderItem{OrderID: "bench-codec-upsert", ItemID: "bench-codec-item", Quantity: 1}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.Quantity = i
		if err := manager.Upsert(ctx, obj); err != nil {
			b.Fatalf("upsert: %v", err)
		}
	}
}

func BenchmarkSelectByPartitionKeyCodec(b *testing.B) {
	ctx := context.Background()
	manager := codecManager(b)

	for i := 0; i < 200; i++ {
		obj := &OrderItem{OrderID: "bench-codec-select", ItemID: fmt.Sprintf("item-%03d", i), Quantity: i}
		if err := manager.Upsert(ctx, obj); err != nil {
			b.Fatalf("seeding: %v", err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		total := 0
		err := manager.SelectByPartitionKey(ctx, func(ctx context.Context, records []*OrderItem, prev []byte, next []byte) (bool, error) {
			total += len(records)
			return true, nil
		}, []tables.QueryOption{tables.WithPaging(100, nil)}, "bench-codec-select")
		if err != nil {
			b.Fatalf("select: %v", err)
		}
		if total != 200 {
			b.Fatalf("expected 200 records, got %d", total)
		}
	}
}

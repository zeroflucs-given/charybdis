package tables_test

import (
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zeroflucs-given/charybdis/tables"
)

func benchOrderManager(b *testing.B) tables.TableManager[Order] {
	b.Helper()
	manager, err := tables.NewTableManager[Order](context.Background(),
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrdersTableSpec),
	)
	if err != nil {
		b.Fatalf("creating table manager: %v", err)
	}
	return manager
}

func benchOrderItemManager(b *testing.B) tables.TableManager[OrderItem] {
	b.Helper()
	manager, err := tables.NewTableManager[OrderItem](context.Background(),
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec),
	)
	if err != nil {
		b.Fatalf("creating table manager: %v", err)
	}
	return manager
}

func BenchmarkGetByPartitionKey(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderManager(b)

	seed := &Order{OrderID: "bench-get-1", ShippingAddress: testAddress(1, "Bench Street", "Benchville")}
	if err := manager.Upsert(ctx, seed); err != nil {
		b.Fatalf("seeding: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.GetByPartitionKey(ctx, "bench-get-1"); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkGetByPrimaryKey(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderItemManager(b)

	seed := &OrderItem{OrderID: "bench-item-order", ItemID: "bench-item-1", Quantity: 3}
	if err := manager.Upsert(ctx, seed); err != nil {
		b.Fatalf("seeding: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := manager.GetByPrimaryKey(ctx, "bench-item-order", "bench-item-1"); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

// BenchmarkGetUsingOptionsPointGet mirrors the shape of crudli-generated Get*ByKey calls,
// which pass one WithKey option per primary key column.
func BenchmarkGetUsingOptionsPointGet(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderItemManager(b)

	seed := &OrderItem{OrderID: "bench-item-order", ItemID: "bench-item-1", Quantity: 3}
	if err := manager.Upsert(ctx, seed); err != nil {
		b.Fatalf("seeding: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := manager.GetUsingOptions(ctx,
			tables.WithKey("order_id", "bench-item-order"),
			tables.WithKey("item_id", "bench-item-1"),
		)
		if err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkUpsert(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderItemManager(b)

	obj := &OrderItem{OrderID: "bench-upsert-order", ItemID: "bench-upsert-item", Quantity: 1}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.Quantity = i
		if err := manager.Upsert(ctx, obj); err != nil {
			b.Fatalf("upsert: %v", err)
		}
	}
}

func BenchmarkInsertOrReplace(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderItemManager(b)

	obj := &OrderItem{OrderID: "bench-ior-order", ItemID: "bench-ior-item", Quantity: 1}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.Quantity = i
		if err := manager.InsertOrReplace(ctx, obj); err != nil {
			b.Fatalf("insert or replace: %v", err)
		}
	}
}

func BenchmarkUpdate(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderItemManager(b)

	obj := &OrderItem{OrderID: "bench-update-order", ItemID: "bench-update-item", Quantity: 1}
	if err := manager.Upsert(ctx, obj); err != nil {
		b.Fatalf("seeding: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		obj.Quantity = i
		if err := manager.Update(ctx, obj); err != nil {
			b.Fatalf("update: %v", err)
		}
	}
}

func BenchmarkDeleteByPrimaryKey(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderItemManager(b)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := manager.DeleteByPrimaryKey(ctx, "bench-delete-order", "bench-delete-item"); err != nil {
			b.Fatalf("delete: %v", err)
		}
	}
}

func BenchmarkSelectByPartitionKey(b *testing.B) {
	ctx := context.Background()
	manager := benchOrderItemManager(b)

	for i := range 200 {
		obj := &OrderItem{OrderID: "bench-select-order", ItemID: fmt.Sprintf("item-%03d", i), Quantity: i}
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
		}, []tables.QueryOption{tables.WithPaging(100, nil)}, "bench-select-order")
		if err != nil {
			b.Fatalf("select: %v", err)
		}
		if total != 200 {
			b.Fatalf("expected 200 records, got %d", total)
		}
	}
}

// BenchmarkUpsertBulk writes a fixed batch per iteration and reports the peak goroutine
// count observed, to expose the cost of the dispatch strategy as well as per-row allocations.
func BenchmarkUpsertBulk(b *testing.B) {
	const batchSize = 5000

	ctx := context.Background()
	manager := benchOrderItemManager(b)

	instances := make([]*OrderItem, batchSize)
	for i := range instances {
		instances[i] = &OrderItem{
			OrderID:  fmt.Sprintf("bench-bulk-%04d", i%100),
			ItemID:   fmt.Sprintf("item-%04d", i),
			Quantity: i,
		}
	}

	var peak atomic.Int64
	stop := make(chan struct{})
	go func() {
		ticker := time.NewTicker(200 * time.Microsecond)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				if n := int64(runtime.NumGoroutine()); n > peak.Load() {
					peak.Store(n)
				}
			}
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := manager.UpsertBulk(ctx, instances, 0); err != nil {
			b.Fatalf("bulk upsert: %v", err)
		}
	}
	b.StopTimer()
	close(stop)
	b.ReportMetric(float64(peak.Load()), "peak-goroutines")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(batchSize), "ns/row")
}

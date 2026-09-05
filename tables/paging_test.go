package tables_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/zeroflucs-given/charybdis/tables"
)

// TestPagedSelectExactDelivery checks that multi-page reads deliver every row exactly once,
// including when the caller passes no paging options at all.
func TestPagedSelectExactDelivery(t *testing.T) {
	const numRows = 250 // Three pages at the default page size of 100

	ctx := context.Background()
	manager, err := tables.NewTableManager[OrderItem](ctx,
		tables.WithCluster(testClusterConfig),
		tables.WithKeyspace(TestKeyspace),
		tables.WithTableSpecification(OrderItemsTableSpec))
	require.NoError(t, err, "Should not error starting up")

	seed := make([]*OrderItem, numRows)
	for i := range seed {
		seed[i] = &OrderItem{
			OrderID:  "paging-test-order",
			ItemID:   fmt.Sprintf("paging-item-%03d", i),
			Quantity: i + 1,
		}
	}
	require.NoError(t, manager.UpsertBulk(ctx, seed, 0), "Should not error seeding")

	collect := func() (map[string]int, *int, tables.PageHandlerFn[OrderItem]) {
		seen := map[string]int{}
		pages := 0
		return seen, &pages, func(ctx context.Context, records []*OrderItem, prev []byte, next []byte) (bool, error) {
			pages++
			for _, r := range records {
				if strings.HasPrefix(r.ItemID, "paging-item-") {
					seen[r.ItemID]++
				}
			}
			return true, nil
		}
	}

	requireExact := func(t *testing.T, seen map[string]int, pages int) {
		require.Len(t, seen, numRows, "Should see every row")
		require.GreaterOrEqual(t, pages, 3, "Should deliver across multiple pages")
		for id, count := range seen {
			require.Equalf(t, 1, count, "Row %s should be delivered exactly once", id)
		}
	}

	t.Run("SelectByPartitionKeyNoOptions", func(t *testing.T) {
		seen, pages, fn := collect()
		require.NoError(t, manager.SelectByPartitionKey(ctx, fn, nil, "paging-test-order"))
		requireExact(t, seen, *pages)
	})

	t.Run("SelectByPartitionKeyWithPaging", func(t *testing.T) {
		seen, pages, fn := collect()
		opts := []tables.QueryOption{tables.WithPaging(100, nil)}
		require.NoError(t, manager.SelectByPartitionKey(ctx, fn, opts, "paging-test-order"))
		requireExact(t, seen, *pages)
	})

	t.Run("SelectByPrimaryKeyNoOptions", func(t *testing.T) {
		seen, pages, fn := collect()
		require.NoError(t, manager.SelectByPrimaryKey(ctx, fn, nil, "paging-test-order"))
		requireExact(t, seen, *pages)
	})

	t.Run("ScanNoOptions", func(t *testing.T) {
		seen, pages, fn := collect()
		require.NoError(t, manager.Scan(ctx, fn))
		requireExact(t, seen, *pages)
	})

	// Drive paging the way crudli-generated repositories do: one page per call, resuming
	// via the page state passed back through WithPaging. Covers both a partial final page
	// and an exactly-full final page, and must terminate.
	for _, pageSize := range []int{100, 50} {
		t.Run(fmt.Sprintf("ResumedSinglePageLoopSize%d", pageSize), func(t *testing.T) {
			seen := map[string]int{}
			var state []byte
			pages := 0
			for {
				var scanner tables.SinglePageScanner[OrderItem]
				opts := []tables.QueryOption{tables.WithPaging(pageSize, state)}
				require.NoError(t, manager.SelectByPartitionKey(ctx, scanner.OnPage, opts, "paging-test-order"))
				pages++
				require.LessOrEqual(t, pages, numRows/pageSize+2, "Resumed paging must terminate")
				for _, r := range scanner.Result() {
					if strings.HasPrefix(r.ItemID, "paging-item-") {
						seen[r.ItemID]++
					}
				}
				state = scanner.PageState()
				if len(state) == 0 {
					break
				}
			}
			require.Len(t, seen, numRows, "Should see every row")
			for id, count := range seen {
				require.Equalf(t, 1, count, "Row %s should be delivered exactly once", id)
			}
		})
	}
}

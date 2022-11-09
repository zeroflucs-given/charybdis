package tables

import "context"

// GreedyScanner is a helper type that lets us read all records from a table without iterating. It
// is essentally able have its OnPage function passed as the PageHandlerFn to other functions.
type GreedyScanner[T any] struct {
	items []*T
}

// Preallocate ensures the slice exists, pre-allocated to a given size
func (g *GreedyScanner[T]) Preallocate(cap int) {
	g.items = make([]*T, 0, cap)
}

// OnPage is a PageHandlerFn that always keeps requesting more data
func (g *GreedyScanner[T]) OnPage(ctx context.Context, records []*T, originalPagingState []byte, newPagingState []byte) (bool, error) {
	g.items = append(g.items, records...)
	return true, ctx.Err()
}

// Result of the scan operation
func (g *GreedyScanner[T]) Result() []*T {
	return g.items
}

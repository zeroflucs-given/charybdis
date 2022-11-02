package tables

import "context"

// SinglePageScanner reads a single page of records and stops iteration.
type SinglePageScanner[T any] struct {
	items []*T
}

// Preallocate ensures the slice exists, pre-allocated to a given size
func (g *SinglePageScanner[T]) Preallocate(cap int) {
	g.items = make([]*T, 0, cap)
}

// OnPage is a PageHandlerFn that stops after a single page
func (g *SinglePageScanner[T]) OnPage(ctx context.Context, records []*T, pageState []byte) (bool, error) {
	g.items = append(g.items, records...)
	return false, ctx.Err()
}

// Result of the scan operation
func (g *SinglePageScanner[T]) Result() []*T {
	return g.items
}

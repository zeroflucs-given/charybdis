package tables

import "context"

// SinglePageScanner reads a single page of records and stops iteration.
type SinglePageScanner[T any] struct {
	lastPageState    []byte
	currentPageState []byte
	items            []*T
}

// Preallocate ensures the slice exists, pre-allocated to a given size
func (g *SinglePageScanner[T]) Preallocate(cap int) {
	g.items = make([]*T, 0, cap)
}

// OnPage is a PageHandlerFn that stops after a single page
func (g *SinglePageScanner[T]) OnPage(ctx context.Context, records []*T, originalPagingState []byte, newPagingState []byte) (bool, error) {
	g.items = append(g.items, records...)
	g.lastPageState = originalPagingState
	g.currentPageState = newPagingState
	return false, ctx.Err()
}

// Result of the scan operation
func (g *SinglePageScanner[T]) Result() []*T {
	return g.items
}

// PageState returns the current page state of the scanner
func (g *SinglePageScanner[T]) PageState() []byte {
	return g.currentPageState
}

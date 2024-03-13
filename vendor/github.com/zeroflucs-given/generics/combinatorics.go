package generics

import (
	"github.com/zeroflucs-given/generics/filtering"
)

// IndexedItem is an item that is indexed
type IndexedItem[T any] struct {
	Index int `json:"index"`
	Item  T   `json:"item"`
}

// Combinations generates all combinations of the input objects at the specified size.
func Combinations[T any](items []T, size int) [][]T {
	if size == 0 || size > len(items) {
		return nil
	}

	var result [][]T
	for i, item := range items {
		if size == 1 {
			result = append(result, []T{item})
		} else {
			rest := items[i+1:]
			for _, comb := range Combinations(rest, size-1) {
				result = append(result, append([]T{item}, comb...))
			}
		}
	}

	return result
}

// CombinationsFiltered returns the combinations of items, but applies a filter to the items. The returned indexed
// items represent the original positions in the raw list.
func CombinationsFiltered[T any](items []T, size int, filter filtering.Expression[T]) [][]IndexedItem[T] {
	filteredItems := make([]IndexedItem[T], 0, len(items))
	for i, v := range items {
		if filter == nil || filter(i, v) {
			filteredItems = append(filteredItems, IndexedItem[T]{
				Index: i,
				Item:  v,
			})
		}
	}

	return Combinations(filteredItems, size)
}

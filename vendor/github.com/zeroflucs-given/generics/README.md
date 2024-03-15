# generics
_Generic code helpers for Go 1.18+ onwards_

[![GoDoc](https://godoc.org/github.com/zeroflucs-given/generics?status.svg)](https://godoc.org/github.com/zeroflucs-given/generics)
![GitHub issues](https://img.shields.io/github/issues/zeroflucs-given/generics)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
![GitHub commit activity](https://img.shields.io/github/commit-activity/y/zeroflucs-given/generics)

---

### Overview
The `generics` package is a series of helpers for writing generic Go code that provides a series of functions that don't exist
in the mainline package set.

## About ZeroFlucs 
[ZeroFlucs](https://zeroflucs.io) is a B2B provider of pricing technology for Sportsbooks/wagering service providers globally. We
use Open-Source software through our platform stack. This, along with other projects is made available through our _zeroflucs-given_ 
Github profile on MIT licensing. To learn more you can visit:

- [The ZeroFlucs Website](https://zeroflucs.io) - For information about our products and services.
- [The ZeroFlucs Team Blog](https://blog.zeroflucs.io/) - For more content and posts from the ZeroFlucs team.
- [ZeroFlucs-Given](https://github.com/zeroflucs-given/) - For more OSS contributions.

## Why Does this Exist?


When writing Go code for Go 1.17 or below, we've all written more than our fair share of methods to check "does this slice contain a thing", or "give me the first item matching a predicate". This package contains a roll-up of helpers and methods, as well as generic collection types that enable a lot of this boilerplate code to be removed.

Key attributes:
 
  - Context support for filters/mappers (Optional)
  - Does not mutate input during operation.

All code is covered 100% by tests for expected behaviour. Where filters or mappers are used
methods are provided with and without context support.

# Slice Queries

```
package generics_test

import (
	"fmt"

	"github.com/zeroflucs-given/generics/query"
)

func main() {
	inputs := []int{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	}
	query := query.Slice(inputs)

	result := query.
		Skip(5).
		TakeUntil(func(index int, v int) bool {
			return v >= 15
		}).
		Filter(func(index int, v int) bool {
			return v%2 == 0 // Evens only
		}).
		Reverse().
        ToSlice() // Back to a clean slice type

	fmt.Println(result) // [14, 12, 10, 8, 6]
}
```

__But what about Contexts?__

Don't worry, we've got you. Use `.WithContext(ctx)` on the query chain and the entire subtree is now context aware. Any materialization functions that emit an actual value will now return an extra `error` argument. Operations are lazy, and will skip over the remainder of any query chain once the first error has occured.

```
package generics_test

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/generics/query"
)

func main() {
	inputs := []int{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
	}
	query := query.Slice(inputs)
	ctx := context.Todo()

	result, err := query.
		WithContext(ctx).
		Skip(5).
		TakeUntil(func(ctx context.Context, index int, v int) (bool, error) {
			return v >= 15, nil
		}).
		Filter(func(ctx context.Context, index int, v int) (bool, error)  {
			return v%2 == 0, nil // Evens only
		}).
		Reverse().
        ToSlice() // Back to a clean slice type

	fmt.Println(result) // [14, 12, 10, 8, 6]
	fmt.Println(err) // nil
}
```

When dealing with slice operations, the following design rules apply:

- Operations that return a single value will return the type default if no item matches (i.e. 0 for numeric types, empty string or nil for objects)
- Operations that accept multiple filters combine these filters into a logical AND by default.
- If no filters are applied, every item is assumed to pass.
- Context aware operations only exist where it makes sense (i.e. Take first 5 doesn't need a context, whereas take with filtering does)

## Operation Detail
### All / AllContext
Returns true if all items in the slice match the filter. Will return true for an empty
slice as no items fail the predicate.

### Any / AnyContext
Returns true if any item in the slice passes.

#### Combinations
Returns all combinations (note: not permutations) of items of length N over the slice.

### CombinationsFiltered 
Returns all combinations of items of length N over the slice, where the members of the slice can be filtered.
The return type contains references back to the original input list indicies. 

### Concatenate
Joins N slices of items together in the given order. Allocates a new slice.

### Contains
Returns true if the slice contains the specified value T, false otherwise. Uses standard equality operator to compare types.

### Count / CountContext
Returns the count of items matching the input filters.

### Cut
Takes a slice and returns the head of the slice, plus a second value for the remainder
of the slice.

### DefaultIfEmpty 
Given a slice, if the slice is empty or nil will create a slice of a single default item.

### Distinct
Sorts and removes any duplicate elements from a slice, returning a new copy. The input slice is unchanged.

### DistinctFunc
Similar to Distinct but takes a custom comparison function.

### DistinctStable
Similar to Distinct but keeps the original order of the elements.

### DistinctStableFunc
Similar to DistinctStable but takes a custom hash function. Hash collisions are ignored.

### ExecuteOnce
Takes a function to be run at a later time and caches its result for retrieval many times.
Subsequent retrievals will block until either their context is cancelled, or the task completes.

### Filter / FilterContext
Creates a filtered set of the values in the slice, using a filter function.

### First / FirstWithContext
Returns the first item of the slice that matches the filters. If no value matches, returns
the type default.

### FirstIndexOf
Returns the first index of a value in a typed slice, or -1 if not present.

### Group / GroupWithContext
Uses a mapper function to assign input values to buckets.

### If
If returns the equivalent value based on the predicate. This is an eager evaluation of both sides
and not a true ternary operator.

### Last / LastWithContext
Returns the last item of the slice that matches the filter.

### LastIndexOf
Returns the index of the last occurrence of a value in the slice, or -1 if not present.

### Map / MapWithContext
Runs the specified mapper over each element of the input slice, creating an output slice of
a different type.

### Mutate
Allows mutation of the slice elements, but the output must be of the same type as the original 
elements. 

### PointerTo
Returns a pointer reference to the input. Useful for lots of APIs that use string, integer pointers to differentiate between empty and absent.

## PointerOrNil 
Returns a pointer to the input, unless the input is the default value for its type (i.e. 0, empty string etc). In that scenario will
return nil.

### Reverse
Creates a reverse-sorted version of the input slice.

### Skip
Skip the first N elements of the slice.

### SkipUntil / SkipUntilWithContext
Skip items from the front of the slice until the predicate returns true.

### SkipWhile / SkipWhileWithContext
Skips items from the front of the slice until the predicate returns false.

### Take
Take the next N elements of the slice.

### TakeUntil / TakeUntil
Take items from the slice until the filter function returns true.

### TakeWhile / TakeWhileWithContext
Take items from the slice until the filter function returns false.

### ToMap / ToMapWithContext
Converts a slice of values to Go map, using mappers for the key and values respectively.

### ValueOrDefault
If a pointer is set, will return the dereferenced value. Otherwise returns the default value of the target type.

## Slice Aggregations
---------------
### Min
Returns the minimum value from the input slice. Returns 0 if no values.

### Max
Returns the maximum value from the input slice. Returns 0 if no values.

### Sum
Returns the total sum of the values. Note that when handling large values, you may overflow your input type.

# Map Operations
The following map operation helpers exist in the `generics` package:

## KeyValuesToMap 
Assembles a map from a slice of key-value pairs.

## Keys
Returns a slice of the key members of the map.

## MapValues / MapValuesWithContext
Allows translation of a maps items to other values.

## ToKeyValues
Converts a map into a slice of key-value pairs. As per Go handling of maps, the order of
output here is not in a guaranteed order.

## Values
Return a slice of the value members of the map.

# Error Checking
## Must
Returns the first value in a pair of (T, error) if there is no error. Otherwise will panic.

# Filter Building
The `filtering` sub-package contains various constructs used to filter values.

### True / TrueWithContext
A constant value filter that always returns true. Useful for testing.

### False / FalseWithContext
A constant value filter that always returns false. Useful for testing.

### And(...filters) / AndWithContext
Creates a composite AND filter for multiple conditions. An empty set of filters is a true.

### Or(...filters) / OrWithContext
Creates a composite OR filter for multiple conditions. An empty set of filters is a false. 

### Not(filter) / NotWithContext
Creates a NOT/inverted version of the specified filter to allow for negative checking.

### Wrap
Takes a non-context aware filter, but makes it compatible with code that expects contexts.

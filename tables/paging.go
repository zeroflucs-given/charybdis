package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v2"
)

// PageHandlerFn is a function used when querying a block of records from the table. If true is returned
// the scan will continue advancing. The page state is an opaque value that can be passed using
// WithPageState to resume a query later.
type PageHandlerFn[T any] func(ctx context.Context, records []*T, pageState []byte) (bool, error)

// QueryBuilderFn is a function used to provide custom query instances to execute.
type QueryBuilderFn func(ctx context.Context) *gocqlx.Queryx

// pageQueryInternal performs paging of a query
func (t *baseManagerImpl[T]) pageQueryInternal(ctx context.Context, queryBuilder QueryBuilderFn, fn PageHandlerFn[T], opts ...QueryOption) error {
	var pageState []byte

	for {
		query := queryBuilder(ctx).
			Consistency(t.readConsistency).
			PageSize(DefaultPageSize)

		// Apply query options that can override any of the above
		for _, opt := range opts {
			query = opt.applyToQuery(query)
		}
		if pageState != nil {
			query = query.PageState(pageState)
		}

		iter := query.Iter()

		records, updatedPageState, err := t.fetchOnePage(ctx, iter)
		query.Release()

		if err != nil {
			return err
		} else if len(records) == 0 {
			break
		}

		keepGoing, errHandle := fn(ctx, records, pageState)
		if errHandle != nil {
			return errHandle
		}

		// If we're stopping, or there's no additional paging state
		if !keepGoing || len(updatedPageState) == 0 {
			break
		}

		// Carry on from next page
		pageState = updatedPageState
	}

	return nil
}

// fetchOnePage fetches a single page of a paged query
func (t *baseManagerImpl[T]) fetchOnePage(ctx context.Context, iter *gocqlx.Iterx) ([]*T, []byte, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	var result []*T
	return result, iter.PageState(), iter.Select(&result)
}

package tables

import (
	"context"

	"github.com/scylladb/gocqlx/v3"
)

// PageHandlerFn is a function used when querying a block of records from the table. If true is returned
// the scan will continue advancing. The page state is an opaque value that can be passed using
// WithPageState to resume a query later.
type PageHandlerFn[T any] func(ctx context.Context, records []*T, originalPagingState []byte, newPagingState []byte) (bool, error)

// QueryBuilderFn is a function used to provide custom query instances to execute.
type QueryBuilderFn func(ctx context.Context, sess gocqlx.Session) *gocqlx.Queryx

// pageQueryInternal performs paging of a query. Codec scanning applies only to statements this
// manager built with its full column list, never to caller-supplied custom statements.
func (t *baseManagerImpl[T]) pageQueryInternal(ctx context.Context, queryBuilder QueryBuilderFn, fn PageHandlerFn[T], useCodec bool, opts ...QueryOption) error {
	useCodec = useCodec && t.codec != nil && !optionsProjectColumns(opts)

	var pageState []byte

	for {
		// The initial nil page state matters: gocql only disables automatic paging once a page
		// state is applied, and without it the first iterator consumes every page and the loop
		// re-delivers everything beyond page one. Options may then override it with a caller's
		// resume state, and later iterations advance using the state from the previous page.
		query := queryBuilder(ctx, t.Session).
			Consistency(t.readConsistency).
			PageSize(DefaultPageSize).
			PageState(nil)

		// Apply query options that can override any of the above
		for _, opt := range opts {
			if opt == nil {
				continue
			}
			query = opt.applyToQuery(query)
		}

		if pageState != nil {
			query = query.PageState(pageState)
		}

		// Check for any binding errors
		if query.Err() != nil {
			return query.Err()
		}

		iter := query.Iter()

		records, updatedPageState, err := t.fetchOnePage(ctx, iter, useCodec)
		query.Release()

		if err != nil {
			return err
		} else if len(records) == 0 {
			break
		}

		keepGoing, errHandle := fn(ctx, records, pageState, updatedPageState)
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
func (t *baseManagerImpl[T]) fetchOnePage(ctx context.Context, iter *gocqlx.Iterx, useCodec bool) ([]*T, []byte, error) {
	if ctx.Err() != nil {
		return nil, nil, ctx.Err()
	}

	if useCodec {
		pageState := iter.PageState()
		result := make([]*T, 0, iter.NumRows())
		scratch := make([]any, len(t.allColumnNames))
		for {
			row, ok := t.codec.ScanNext(iter.Iter, scratch)
			if !ok {
				break
			}
			result = append(result, row)
		}
		return result, pageState, iter.Close()
	}

	var result []*T
	return result, iter.PageState(), iter.Select(&result)
}

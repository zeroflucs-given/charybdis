package tables

import (
	"context"

	"github.com/gocql/gocql"
)

// RowCodec provides generated, reflection-free row scanning and binding for a row type.
// Implementations are generated from the table specification and must present columns in
// specification order; managers verify this at construction and fall back to reflection
// based mapping when the column sets disagree.
type RowCodec[T any] interface {
	// Columns is the specification-order column list the codec was generated against
	Columns() []string
	// ScanNext reads the next row from the iterator, returning false when no row was read.
	// The scratch slice is caller-provided scan-destination memory of at least len(Columns()),
	// reusable across rows; implementations must not retain it.
	ScanNext(iter *gocql.Iter, scratch []any) (*T, bool)
	// BindColumns returns the values of the named columns, in name order
	BindColumns(v *T, names []string) []any
}

// WithRowCodec supplies a generated row codec for the managed type, replacing reflection
// based scanning and binding on operations whose column set is the full specification.
func WithRowCodec[T any](codec RowCodec[T]) ManagerOption {
	return &tableManagerOption{
		parametersHook: func(ctx context.Context, params *tableManagerParameters) error {
			params.RowCodec = codec
			return nil
		},
	}
}

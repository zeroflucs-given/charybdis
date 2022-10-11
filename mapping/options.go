package mapping

import (
	"github.com/zeroflucs-given/charybdis/tables"
)

// WithAutomaticSpecification creates a table-manager option that sets the table
// specification by reflecting over the structure.
func WithAutomaticSpecification[T any](name string) tables.TableManagerOption {
	var instance T
	spec, err := CreateTableSpecificationFromExample(name, &instance)

	// This is a little unclean, but in my heart I'm comfortable
	// with this as tag errors are clearly a compile-time issue and
	// can't be "handled".
	if err != nil {
		panic(err)
	}

	return tables.WithTableSpecification(spec)
}

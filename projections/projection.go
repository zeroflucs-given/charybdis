package projections

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/charybdis/tables"
)

// projectionImpl is an individual projection known to the system.
type projectionImpl[T any] struct {
	projectionKeyEx PrimaryKeyExtractor    // Function to extract primary key of control table
	leafTable       tables.TableManager[T] // The table that stores the data in its final form
}

// HasExisting returns true, if the record already exists in the leaf table
func (p *projectionImpl[T]) HasExisting(ctx context.Context, control *T) (bool, error) {
	projectionKey, err := p.projectionKeyEx(control)
	if err != nil {
		return false, fmt.Errorf("error extracting projection key: %w", err)
	}

	existing, err := p.leafTable.GetByPrimaryKey(ctx, projectionKey...)
	return existing != nil, err
}

// Delete cleans out the existing projected data
func (p *projectionImpl[T]) Delete(ctx context.Context, instance *T) error {
	return p.leafTable.Delete(ctx, instance)
}

// ProcessChange stores the value into our projection table
func (p *projectionImpl[T]) ProcessChange(ctx context.Context, instance *T) error {
	return p.leafTable.Upsert(ctx, instance)
}

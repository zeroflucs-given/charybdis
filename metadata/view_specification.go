package metadata

import (
	"fmt"

	"github.com/zeroflucs-given/generics"
)

// ViewSpecification is a specification of a view.
type ViewSpecification struct {
	Name         string                `json:"name"`         // Name of the view to create.
	Table        *TableSpecification   `json:"table"`        // Table we are a view of
	Partitioning []*PartitioningColumn `json:"partitioning"` // Partitioning keys
	Clustering   []*ClusteringColumn   `json:"clustering"`   // Clustering keys
}

// Validate the table specification
func (v *ViewSpecification) Validate() error {
	if v == nil || v.Table == nil {
		return ErrNoObject
	}

	// Check names
	if !isValidName(v.Name) {
		return fmt.Errorf("%w: %q", ErrInvalidTableOrViewName, v.Name)
	}

	// Table must be valid
	if err := v.Table.Validate(); err != nil {
		return err
	}

	// All view key columns must be part of the table key, with at most one extra
	tableKeys := generics.Concatenate(
		generics.Map(v.Table.Partitioning, func(i int, p *PartitioningColumn) string {
			return p.Column.Name
		}),
		generics.Map(v.Table.Clustering, func(i int, c *ClusteringColumn) string {
			return c.Column.Name
		}),
	)
	viewKeys := generics.Concatenate(
		generics.Map(v.Partitioning, func(i int, p *PartitioningColumn) string {
			return p.Column.Name
		}),
		generics.Map(v.Clustering, func(i int, c *ClusteringColumn) string {
			return c.Column.Name
		}),
	)
	if len(tableKeys) > len(viewKeys) {
		return fmt.Errorf("too many view keys: have %d but table only has %d: %w", len(viewKeys), len(tableKeys), ErrViewKeyUnsuitable)
	}

	intersected := generics.Except(viewKeys, tableKeys...)
	if len(intersected) > 1 {
		return fmt.Errorf("too many residual keys: %v - %w", intersected, ErrViewKeyUnsuitable)
	}

	return nil
}

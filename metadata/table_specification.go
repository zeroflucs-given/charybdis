package metadata

import (
	"fmt"

	"github.com/scylladb/gocqlx/v2/table"
	"github.com/zeroflucs-given/generics"
)

// TableSpecification is a description of a table.
type TableSpecification struct {
	Name         string                          `json:"name"`         // Name of the table?
	Columns      []*ColumnSpecification          `json:"columns"`      // Columns of the table
	Partitioning []*PartitioningColumn           `json:"partitioning"` // Partitioning keys
	Clustering   []*ClusteringColumn             `json:"clustering"`   // Clustering keys
	Indexes      map[string]*ColumnSpecification `json:"indexes"`      // Indexes to create
}

// ToCQLX converts this tablespec to a go-cqlx friendly metadata object.
func (t *TableSpecification) ToCQLX() *table.Table {
	if t == nil {
		return nil
	}

	md := table.Metadata{
		Name: t.Name,
		Columns: generics.Map(t.Columns, func(i int, v *ColumnSpecification) string {
			return v.Name
		}),
		PartKey: generics.Map(t.Partitioning, func(i int, v *PartitioningColumn) string {
			return v.Column.Name
		}),
		SortKey: generics.Map(t.Clustering, func(i int, v *ClusteringColumn) string {
			return v.Column.Name
		}),
	}

	return table.New(md)
}

// Validate the table specification
func (t *TableSpecification) Validate() error {
	if t == nil {
		return ErrNoObject
	}

	// Check names
	if !isValidName(t.Name) {
		return fmt.Errorf("%w: %q", ErrInvalidColumnName, t.Name)
	}

	// Check all columns
	for _, col := range t.Columns {
		err := col.Validate()
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}

		// We can't be both a clustering and partition key
		if col.IsClusteringKey && col.IsPartitioningKey {
			return fmt.Errorf("column %q: %w", col.Name, ErrInconsistentMetadata)
		}
	}

	// We require a partitioning key
	if len(t.Partitioning) == 0 {
		return ErrNoPartitioningKey
	}

	// Check partitioning column reference consistency
	for _, pc := range t.Partitioning {
		found := false
		for _, col := range t.Columns {
			if col == pc.Column {
				found = true
				if !col.IsPartitioningKey {
					return fmt.Errorf("column %q: %w", col.Name, ErrInconsistentMetadata)
				}
			}
		}
		if !found {
			return ErrMismatchedColumns
		}
	}

	// Check clustering column reference consistency
	for _, cc := range t.Clustering {
		found := false
		for _, col := range t.Columns {
			if col == cc.Column {
				found = true
				if !col.IsClusteringKey {
					return fmt.Errorf("column %q: %w", col.Name, ErrInconsistentMetadata)
				}
			}
		}
		if !found {
			return ErrMismatchedColumns
		}
	}

	// Check index column reference consistency (indexed columns need to
	// by in our column list)
	for _, ixCol := range t.Indexes {
		found := false
		for _, col := range t.Columns {
			if col == ixCol {
				found = true
			}
		}
		if !found {
			return ErrMismatchedColumns
		}
	}

	return nil
}

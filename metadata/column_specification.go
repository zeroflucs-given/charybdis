package metadata

import "fmt"

// ColumnSpecification is a specification that describes a column
type ColumnSpecification struct {
	Name              string `json:"name"`            // Name of the column
	CQLType           string `json:"cql_type"`        // The CQL Type string for this column
	IsPartitioningKey bool   `json:"is_partitioning"` // Partitioning key?
	IsClusteringKey   bool   `json:"is_clustering"`   // Clustering key?
}

// Validate the column specification
func (c *ColumnSpecification) Validate() error {
	if c == nil {
		return ErrNoObject
	}

	// Check names
	if !isValidName(c.Name) {
		return fmt.Errorf("%w: %q", ErrInvalidColumnName, c.Name)
	}

	// We should have a CQL type
	if c.CQLType == "" {
		return ErrInconsistentMetadata
	}

	return nil
}

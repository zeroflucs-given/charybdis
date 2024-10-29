package metadata

import "fmt"

// FieldSpecification is a specification that describes a field of a type
type FieldSpecification struct {
	Name    string `json:"name"`     // Name of the field
	CQLType string `json:"cql_type"` // The CQL Type string for this field
}

// Validate the column specification
func (c *FieldSpecification) Validate() error {
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

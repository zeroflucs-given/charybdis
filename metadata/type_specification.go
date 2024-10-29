package metadata

import (
	"fmt"

	"github.com/gocql/gocql"

	"github.com/zeroflucs-given/generics"
)

// TypeSpecification is a specification of a type.
type TypeSpecification struct {
	Keyspace string                `json:"keyspace"` // The Keyspace the type is created in
	Name     string                `json:"name"`     // Name of the type to create.
	Fields   []*FieldSpecification `json:"fields"`   // Fields in the type
}

// Validate the table specification
func (v *TypeSpecification) Validate() error {
	if v == nil {
		return ErrNoObject
	}

	// Check names
	if !isValidName(v.Name) {
		return fmt.Errorf("%w: %q", ErrInvalidTableOrViewName, v.Name)
	}

	// Must be at least one field
	if len(v.Fields) == 0 {
		return fmt.Errorf("%w: %q", ErrNoFields, v.Name)
	}

	// Check fields
	for _, field := range v.Fields {
		if err := field.Validate(); err != nil {
			return err
		}
	}

	return nil
}

func (v *TypeSpecification) Clone() *TypeSpecification {
	return &TypeSpecification{
		Keyspace: v.Keyspace,
		Name:     v.Name,
		Fields: generics.Map(v.Fields, func(i int, f *FieldSpecification) *FieldSpecification {
			if f == nil {
				return nil
			}
			spec := &FieldSpecification{}
			*spec = *f
			return spec
		}),
	}
}

// ToCQLX converts this tablespec to a go-cqlx friendly metadata object.
func (v *TypeSpecification) ToCQLX() *gocql.TypeMetadata {
	if v == nil {
		return nil
	}

	// The full list of columns needs to be built from the partition and sorting keys and
	// then appending any remaining columns from the base table

	md := &gocql.TypeMetadata{
		Keyspace: v.Keyspace,
		Name:     v.Name,
		FieldNames: generics.Map(v.Fields, func(i int, v *FieldSpecification) string {
			return v.Name
		}),
		FieldTypes: generics.Map(v.Fields, func(i int, v *FieldSpecification) string {
			return v.CQLType
		}),
	}

	// return table.New(md)
	return md
}

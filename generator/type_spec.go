package generator

import (
	"fmt"

	"github.com/gocql/gocql"

	"github.com/zeroflucs-given/charybdis/metadata"
)

// CreateDDLFromTypeSpecification creates the DDL to create a type from its spec
func CreateDDLFromTypeSpecification(keyspace string, spec *metadata.TypeSpecification, existingMetadata *gocql.TypeMetadata) ([]metadata.DDLOperation, error) {
	// Validate input
	if keyspace == "" || spec == nil {
		return nil, ErrInvalidInput
	}

	errSpec := spec.Validate()
	if errSpec != nil {
		return nil, fmt.Errorf("error validating type spec: %w", errSpec)
	}

	var commands []metadata.DDLOperation

	// Create the shell of the type if it does not exist
	if existingMetadata == nil {
		initialCreate := fmt.Sprintf("CREATE TYPE IF NOT EXISTS %v.%v (%s %s)",
			keyspace,
			spec.Name,
			spec.Fields[0].Name, // must always be at least one, else Validate above would have failed
			spec.Fields[0].CQLType,
		)

		commands = append(commands, metadata.DDLOperation{
			Description:  fmt.Sprintf("Create the type %q.", spec.Name),
			Command:      initialCreate,
			IgnoreErrors: []string{},
		})
	}

	fieldSet := getFieldSet(existingMetadata)

	// Create any missing fields
	for _, field := range spec.Fields[1:] {
		if fieldSet.Has(field.Name) {
			continue
		}
		addFieldStmt := fmt.Sprintf("ALTER TYPE %v.%v ADD %s %s", keyspace, spec.Name, field.Name, field.CQLType)

		commands = append(commands, metadata.DDLOperation{
			Description:  fmt.Sprintf("Extend the type %q with the field %q if needed.", spec.Name, field.Name),
			Command:      addFieldStmt,
			IgnoreErrors: []string{MessageColumnExists},
		})
	}

	return commands, nil
}

type setType[E comparable] map[E]struct{}

func (s setType[E]) Has(element E) bool {
	_, has := s[element]
	return has
}

func getFieldSet(m *gocql.TypeMetadata) setType[string] {
	if m == nil {
		return nil
	}
	return asSet(m.FieldNames)
}

func asSet[S ~[]E, E comparable](s S) map[E]struct{} {
	res := make(map[E]struct{})
	for _, e := range s {
		res[e] = struct{}{}
	}
	return res
}

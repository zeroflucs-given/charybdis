package generator

import (
	"fmt"
	"strings"

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

	// Create the type if it does not exist. - Note "ALTER TYPE ADD COLUMN" has been removed from Cassandra/Scylla
	if existingMetadata == nil {
		var fields []string
		for _, f := range spec.Fields {
			fields = append(fields, f.Name+" "+f.CQLType)
		}

		initialCreate := fmt.Sprintf("CREATE TYPE IF NOT EXISTS %v.%v (%s)",
			keyspace,
			spec.Name,
			strings.Join(fields, ", "),
		)

		commands = append(commands, metadata.DDLOperation{
			Description:  fmt.Sprintf("Create the type %q.", spec.Name),
			Command:      initialCreate,
			IgnoreErrors: []string{},
		})
	}

	return commands, nil
}

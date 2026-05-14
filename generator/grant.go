package generator

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/charybdis/metadata"
)

func (g *DefinitionGenerator) Grant(ctx context.Context, subject string, objectType GrantObject, objectName string, verbs ...GrantVerb) error {
	ddl, errDDL := CreateGrantDDL(subject, objectType, objectName, verbs...)
	if errDDL != nil {
		return errDDL
	}
	return installDLL(ctx, g.logger, g.session, ddl)
}

func CreateGrantDDL(subject string, objectType GrantObject, objectName string, verbs ...GrantVerb) ([]metadata.DDLOperation, error) {
	var commands []metadata.DDLOperation

	if err := objectType.Validate(); err != nil {
		return nil, err
	}

	for _, verb := range verbs {
		if err := verb.Validate(); err != nil {
			return nil, err
		}

		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Assigning %s %s permissions to role %s", objectType, verb, objectName),
			Command:     fmt.Sprintf("GRANT %s ON %s %s TO %s", verb, objectType, objectName, subject),
		})
	}

	return commands, nil
}

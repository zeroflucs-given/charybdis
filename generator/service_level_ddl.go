package generator

import (
	"fmt"
	"strings"

	"github.com/zeroflucs-given/charybdis/metadata"
)

func SetServiceLevelExistenceDDL(name string, operation string) ([]metadata.DDLOperation, error) {
	var condition string
	var desc string
	operation = strings.TrimSpace(strings.ToUpper(operation))

	switch operation {
	case "CREATE":
		condition = "NOT EXISTS"
		desc = "Create service level '%s' if it doesn't already exist"
	case "DROP":
		condition = "EXISTS"
		desc = "Drop service level '%s' if it exists"
	default:
		return nil, fmt.Errorf("unknown service level operation '%s': %w", operation, ErrUnknownOperation)
	}

	return []metadata.DDLOperation{
		{
			Description: fmt.Sprintf(desc, name),
			Command:     fmt.Sprintf("%s SERVICE LEVEL IF %s '%s'", operation, condition, EscapeSingleQuote(name)),
		},
	}, nil
}

func UpdateServiceLevelDDL(name string, opts ...ServiceLevelOption) ([]metadata.DDLOperation, error) {
	opt := collectServiceLevelOptions(opts)
	var commands []metadata.DDLOperation

	if opt.shares != nil {
		shares := *opt.shares
		if shares == 0 { // special case 0 as set to default
			shares = 1000
		}

		if shares < 1 || 1000 < shares {
			return nil, fmt.Errorf("cannot set shares of %d on service level '%s': shares must be between 1 and 1000 (inclusive): %w", shares, name, ErrOutOfRange)
		}

		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Assigning %d shares to service level '%s'", shares, name),
			Command:     fmt.Sprintf("ALTER SERVICE LEVEL '%s' WITH SHARES = %d", EscapeSingleQuote(name), shares),
		})
	}

	if opt.timeout != nil {
		timeout := (*opt.timeout).Milliseconds()

		if timeout <= 0 {
			return nil, fmt.Errorf("cannot set a timeout of %v on service level '%s': timeout must be more than 0: %w", timeout, name, ErrOutOfRange)
		}

		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Assigning timeout of '%dms' to service level '%s'", timeout, name),
			Command:     fmt.Sprintf("ALTER SERVICE LEVEL '%s' WITH timeout = %dms", EscapeSingleQuote(name), timeout),
		})
	}

	if opt.workloadType != nil {
		workloadType := *opt.workloadType
		if err := workloadType.Validate(); err != nil {
			return nil, fmt.Errorf("setting workload type for service level '%s': %w", name, err)
		}

		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Assigning workload type '%s' to service level '%s'", workloadType, name),
			Command:     fmt.Sprintf("ALTER SERVICE LEVEL '%s' WITH workload_type = %s", EscapeSingleQuote(name), workloadType),
		})
	}

	return commands, nil
}

func ServiceLevelAttachmentDDL(level, role, operation string) ([]metadata.DDLOperation, error) {
	if err := IsValidIdentifier(role); err != nil {
		return nil, fmt.Errorf("invalid role name: %w", err)
	}

	var preposition string
	operation = strings.TrimSpace(strings.ToUpper(operation))
	switch operation {
	case "ATTACH":
		preposition = "TO"
	case "DETACH":
		preposition = "FROM"
	default:
		return nil, fmt.Errorf("unknown service level operation '%s': %w", operation, ErrUnknownOperation)
	}

	command := metadata.DDLOperation{
		Description: fmt.Sprintf("%sing the service level '%s' %s the role '%s'", strings.ToLower(operation), level, strings.ToLower(preposition), role),
		Command:     fmt.Sprintf("%s SERVICE LEVEL '%s' %s %s", operation, EscapeSingleQuote(level), preposition, role),
	}
	return []metadata.DDLOperation{command}, nil
}

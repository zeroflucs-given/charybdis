package generator

import (
	"context"
	"fmt"

	"github.com/zeroflucs-given/charybdis/metadata"
)

func (g *DefinitionGenerator) CreateRole(ctx context.Context, rolename string, options ...RoleOption) error {
	ddl, errDDL := CreateDDLForRole(rolename, options...)
	if errDDL != nil {
		return errDDL
	}
	return installDLL(ctx, g.logger, g.session, ddl)
}

func (g *DefinitionGenerator) UpdateRole(ctx context.Context, rolename string, options ...RoleOption) error {
	ddl, errDDL := CreateDDLForRole(rolename, append(options, WithCreateIfMissing(false))...)
	if errDDL != nil {
		return errDDL
	}
	return installDLL(ctx, g.logger, g.session, ddl)
}

func CreateDDLForRole(rolename string, options ...RoleOption) ([]metadata.DDLOperation, error) {
	if err := IsValidIdentifier(rolename); err != nil {
		return nil, fmt.Errorf("invalid username: %w", err)
	}

	opts := collectRoleOptions(options)

	var commands []metadata.DDLOperation

	if opts.createIfMissing {
		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Create the role %q if it doesn't already exist", rolename),
			Command:     fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", rolename),
		})
	}

	if opts.password != nil {
		if *opts.password == "" {
			return nil, fmt.Errorf("password cannot be blank: %w", ErrInvalidInput)
		}

		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Set the password for %q as provided", rolename),
			Command:     fmt.Sprintf("ALTER ROLE %s WITH PASSWORD = '%s'", rolename, EscapeSingleQuote(*opts.password)),
		})
	}

	if opts.isSuperuser != nil {
		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Set superuser permissions for %q", rolename),
			Command:     fmt.Sprintf("ALTER ROLE %s WITH SUPERUSER = %t", rolename, *opts.isSuperuser),
		})
	}

	if opts.isLogin != nil {
		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Set login permissions for %q", rolename),
			Command:     fmt.Sprintf("ALTER ROLE %s WITH LOGIN = %t", rolename, *opts.isLogin),
		})
	}

	if opts.serviceLevel != nil {
		cmds, err := ServiceLevelAttachmentDDL(*opts.serviceLevel, rolename, OpAttach)
		if err != nil {
			return nil, err
		}
		commands = append(commands, cmds...)
	}

	return commands, nil
}

type roleOpt struct {
	createIfMissing bool
	password        *string
	isSuperuser     *bool
	isLogin         *bool
	options         map[string]any
	serviceLevel    *string
}

type RoleOption func(opt *roleOpt)

func collectRoleOptions(opts []RoleOption) *roleOpt {
	o := &roleOpt{
		createIfMissing: true,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithCreateIfMissing(create bool) RoleOption {
	return func(opt *roleOpt) {
		opt.createIfMissing = create
	}
}

func WithRolePassword(password string) RoleOption {
	return func(opt *roleOpt) {
		opt.password = ptrTo(password)
	}
}

func WithRoleIsSuperuser(isSuperuser bool) RoleOption {
	return func(opt *roleOpt) {
		opt.isSuperuser = ptrTo(isSuperuser)
	}
}

func WithRoleIsLogin(isLogin bool) RoleOption {
	return func(opt *roleOpt) {
		opt.isLogin = ptrTo(isLogin)
	}
}

func WithRoleOptions(opts map[string]any) RoleOption {
	return func(opt *roleOpt) {
		opt.options = opts
	}
}

// WithRoleServiceLevel attaches the named service level to the role
func WithRoleServiceLevel(serviceLevel string) RoleOption {
	return func(opt *roleOpt) {
		opt.serviceLevel = ptrTo(serviceLevel)
	}
}

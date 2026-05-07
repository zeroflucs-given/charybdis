package generator

import (
	"context"
	"fmt"

	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/charybdis/metadata"
)

func CreateRole(ctx context.Context, logger *zap.Logger, sess gocqlx.Session, rolename string, options ...RoleOption) error {
	ddl, errDDL := CreateDDLForRole(rolename, options...)
	if errDDL != nil {
		return errDDL
	}
	return installDLL(ctx, logger, sess, ddl)
}

func CreateDDLForRole(rolename string, options ...RoleOption) ([]metadata.DDLOperation, error) {
	if err := IsValidIdentifier(rolename); err != nil {
		return nil, fmt.Errorf("invalid username: %w", err)
	}

	var commands []metadata.DDLOperation

	commands = append(commands, metadata.DDLOperation{
		Description: fmt.Sprintf("Create the role %q if it doesn't already exist", rolename),
		Command:     fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", rolename),
	})

	opts := collectRoleOptions(options)

	if opts.password != nil {
		if *opts.password == "" {
			return nil, fmt.Errorf("password is cannot be blank: %w", ErrInvalidInput)
		}

		hash, errHash := HashPassword(*opts.password)
		if errHash != nil {
			return nil, fmt.Errorf("hashing password (len=%d) for user '%s': %w", len(*opts.password), rolename, errHash)
		}

		commands = append(commands, metadata.DDLOperation{
			Description: fmt.Sprintf("Set the password for %q as provided", rolename),
			Command:     fmt.Sprintf("ALTER ROLE %s WITH HASHED PASSWORD '%s'", rolename, hash),
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

	return commands, nil
}

type roleOpt struct {
	password    *string
	isSuperuser *bool
	isLogin     *bool
	options     map[string]any
}

type RoleOption func(opt *roleOpt)

func collectRoleOptions(opts []RoleOption) *roleOpt {
	o := &roleOpt{}
	for _, opt := range opts {
		opt(o)
	}
	return o
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

func ptrTo[T any](n T) *T {
	res := new(T)
	*res = n
	return res
}

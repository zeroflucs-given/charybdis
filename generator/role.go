package generator

import (
	"fmt"
	"strings"

	"github.com/scylladb/gocqlx/v3"
)

func CreateRole(h gocqlx.Session, username string, options ...RoleOption) error {
	if err := IsValidIdentifier(username); err != nil {
		return fmt.Errorf("invalid username: %w", err)
	}

	opts := collectRoleOptions(options)

	cql := fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", username)

	var p []string
	var f []string

	b := make(map[string]any)
	if opts.password != nil {
		f = append(f, "PASSWORD = '"+*opts.password+"'")
		// p = append(p, "password")
		// b["password"] = *opts.password
	}

	if opts.isSuperuser != nil {
		f = append(f, "SUPERUSER = ?")
		p = append(p, "is_superuser")
		b["is_superuser"] = *opts.isSuperuser
	}

	if opts.isLogin != nil {
		f = append(f, "LOGIN = ?")
		p = append(p, "is_login")
		b["is_login"] = *opts.isLogin
	}

	if len(f) > 0 {
		cql += " WITH " + strings.Join(f, " AND ")
	}

	err := h.Query(cql, p).BindMap(b).Exec()

	if err != nil {
		return fmt.Errorf("creating role %q: %w", username, err)
	}

	return nil
}

func AlterRole(h gocqlx.Session, username string, options ...RoleOption) error {
	if err := IsValidIdentifier(username); err != nil {
		return fmt.Errorf("invalid username: %w", err)
	}

	opts := collectRoleOptions(options)

	cql := fmt.Sprintf("ALTER ROLE %s", username)

	var p []string
	var f []string

	b := make(map[string]any)
	if opts.password != nil {
		f = append(f, "PASSWORD = ?")
		p = append(p, "password")
		b["password"] = *opts.password
	}

	if opts.isSuperuser != nil {
		f = append(f, "SUPERUSER = ?")
		p = append(p, "is_superuser")
		b["is_superuser"] = *opts.isSuperuser
	}

	if opts.isLogin != nil {
		f = append(f, "LOGIN = ?")
		p = append(p, "is_login")
		b["is_login"] = *opts.isLogin
	}

	if len(f) > 0 {
		cql += " WITH " + strings.Join(f, " AND ")
	}

	if err := h.Query(cql, p).BindMap(b).Exec(); err != nil {
		return fmt.Errorf("altering role %q: %w", username, err)
	}

	return nil
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

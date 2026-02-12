package generator

import (
	"fmt"
	"strings"

	"github.com/scylladb/gocqlx/v3"
)

type ddlGen struct {
	base string
	err  error
	with []string
}

type DDLGenerator interface {
	With(prop string, val any) DDLGenerator
	Exec(gocqlx.Session) error
}

func CreateRoleX(username string) DDLGenerator {
	if err := IsValidIdentifier(username); err != nil {
		return &ddlGen{
			err: fmt.Errorf("invalid username: %w", err),
		}
	}

	return &ddlGen{
		base: fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s", username),
	}
}

func (g *ddlGen) With(prop string, val any) DDLGenerator {
	g.with = append(g.with, fmt.Sprintf("%s = %q", prop, val))
	return g
}

func (g *ddlGen) Exec(h gocqlx.Session) error {
	if g.err != nil {
		return g.err
	}

	cql := g.base
	if len(g.with) > 0 {
		cql += " WITH " + strings.Join(g.with, " AND ")
	}

	// fmt.Fprintf(os.Stderr, "query: %s", cql)

	return h.ExecStmt(cql)
}

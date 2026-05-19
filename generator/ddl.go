package generator

import (
	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"go.uber.org/zap"
)

// DefinitionGenerator creates DDL objects in Scylla
type DefinitionGenerator struct {
	logger  *zap.Logger
	session gocqlx.Session
}

// NewDefinitionGenerator creates a new initialised DefinitionGenerator from a gocql Session
func NewDefinitionGenerator(logger *zap.Logger, session *gocql.Session) *DefinitionGenerator {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DefinitionGenerator{
		logger:  logger,
		session: gocqlx.NewSession(session),
	}
}

// NewDefinitionGeneratorX creates a new initialised DefinitionGenerator from a gocqlx Session
func NewDefinitionGeneratorX(logger *zap.Logger, session gocqlx.Session) *DefinitionGenerator {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DefinitionGenerator{
		logger:  logger,
		session: session,
	}
}

func (g *DefinitionGenerator) Close() {
	g.session.Close()
}

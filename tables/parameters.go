package tables

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/zeroflucs-given/charybdis/metadata"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// TableManagerParameters is the set of parameters for a table-manager
type tableManagerParameters struct {
	Keyspace         string
	Logger           *zap.Logger
	SessionFactory   SessionFactory
	TracerProvider   trace.TracerProvider
	TableSpec        *metadata.TableSpecification
	ViewSpec         *metadata.ViewSpecification
	ReadConsistency  gocql.Consistency
	WriteConsistency gocql.Consistency
	TTL              time.Duration
}

type SessionFactory func(keyspace string) (*gocql.Session, error)

func (t *tableManagerParameters) ensureDefaults() {
	t.Logger = zap.NewNop()
	t.TracerProvider = trace.NewNoopTracerProvider()
	t.ReadConsistency = gocql.LocalQuorum
	t.WriteConsistency = gocql.LocalQuorum
}

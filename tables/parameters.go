package tables

import (
	"time"

	"github.com/gocql/gocql"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/zap"

	"github.com/zeroflucs-given/charybdis/metadata"
)

// TableManagerParameters is the set of parameters for a table-manager
type tableManagerParameters struct {
	Keyspace         string
	Logger           *zap.Logger
	SessionFactory   SessionFactory
	TracerProvider   trace.TracerProvider
	DoTracing        bool
	TableSpec        *metadata.TableSpecification
	ViewSpec         *metadata.ViewSpecification
	TypeSpecs        []*metadata.TypeSpecification
	ReadConsistency  gocql.Consistency
	WriteConsistency gocql.Consistency
	TTL              time.Duration
	queryTimeout     time.Duration // Populated when the cluster options are set.
}

type SessionFactory func(keyspace string) (*gocql.Session, error)

func (t *tableManagerParameters) ensureDefaults() {
	t.Logger = zap.NewNop()
	t.TracerProvider = noop.NewTracerProvider()
	t.ReadConsistency = gocql.LocalQuorum
	t.WriteConsistency = gocql.LocalQuorum
}

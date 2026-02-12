package tables

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
	"github.com/scylladb/gocqlx/v3/table"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

// baseManagerImpl is our underlying base manager implementation type, common to views and tables
type baseManagerImpl[T any] struct {
	Name            string               // Name of object
	Session         gocqlx.Session       // Session
	Logger          *zap.Logger          // Logger
	Tracer          trace.Tracer         // OpenTelemetry tracer
	DoTracing       bool                 // Do we want to add tracing to operations
	TraceAttributes []attribute.KeyValue // Common trace attributes
	Table           *table.Table         // Table helper
	TableMetadata   table.Metadata       // Table metadata

	// Helper data
	readConsistency        gocql.Consistency // Read consistency
	qualifiedTableName     string            // Qualified table-name
	allColumnNames         []string          // Set of all column names
	nonKeyColumns          []string          // Non-key column names
	partitionKeyPredicates []qb.Cmp          // Partition key predicates
	allKeyPredicates       []qb.Cmp          // All key predicates, including partition key, in order
	queryTimeout           time.Duration     // Timout for queries - copied through from the Session settings
}

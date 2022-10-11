package tables

const (
	// DefaultBulkConcurrency is the number of concurrent updates or actions
	// permitted at once when using bulk operations.
	DefaultBulkConcurrency = 64

	// DefaultPageSize is the number of records fetched in a page.
	DefaultPageSize = 100

	// TracingModuleName is the name of the module to show in any OpenTelemetry
	// trace records for this package.
	TracingModuleName = "charydbis"
)

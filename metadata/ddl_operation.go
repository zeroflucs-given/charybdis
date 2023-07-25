package metadata

// DDLOperation defines the DDL operations to take place
type DDLOperation struct {
	Description  string
	Command      string
	IgnoreErrors []string
}

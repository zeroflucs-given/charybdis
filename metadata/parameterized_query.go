package metadata

// ParameterizedQuery is a structure that describes a parameterized query
type ParameterizedQuery struct {
	Statement  string   `json:"statement"`  // CQL statement
	Parameters []string `json:"parameters"` // Parameters of the query in appearance order
}

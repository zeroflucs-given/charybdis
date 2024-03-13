package metadata

// ClusteringColumn indicates a clustering key column
type ClusteringColumn struct {
	Column     *ColumnSpecification `json:"column"`     // The column we're referring to
	Order      int                  `json:"order"`      // Order of the column
	Descending bool                 `json:"descending"` // Descending order?
}

// ClusteringColumnLookup is a clustering column definition without a reference back
// to the base.
type ClusteringColumnLookup struct {
	Column     string `json:"column"`     // The column we're referring to
	Order      int    `json:"order"`      // Order of the column
	Descending bool   `json:"descending"` // Descending order?
}

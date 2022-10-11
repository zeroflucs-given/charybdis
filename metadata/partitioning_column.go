package metadata

// PartitioningColumn indicates a partitioning key column
type PartitioningColumn struct {
	Column *ColumnSpecification `json:"column"` // The column we're referring to
	Order  int                  `json:"order"`  // Order of the column
}

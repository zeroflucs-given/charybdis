package projections

import (
	"sort"

	"github.com/zeroflucs-given/charybdis/metadata"
)

// ProjectionSpecification is the specification of a projection of a base table. This
// table can have arbitrary additional key members for sorting beyond what the base
// table has, at the cost of being maintained in software.
type ProjectionSpecification struct {
	Name         string                               `json:"name"`            // Name of the projection to create
	Partitioning []*metadata.PartitioningColumnLookup `json:"partition_keys"`  // Partition Keys to use
	Clustering   []*metadata.ClusteringColumnLookup   `json:"clustering_keys"` // Clustering keys to use
}

// Canonicalize the specification into order
func (ps *ProjectionSpecification) Canonicalize() {
	sort.Slice(ps.Partitioning, func(i, j int) bool {
		return ps.Partitioning[i].Order < ps.Partitioning[j].Order
	})
	sort.Slice(ps.Clustering, func(i, j int) bool {
		return ps.Clustering[i].Order < ps.Clustering[j].Order
	})
}

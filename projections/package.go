package projections

// Package projections enables automatic maintainance of non-key aligned projections of Scylla tables.
// Materialized views in Scylla do not support more than one non-key column when laying out data, nor
// are they capable of handling time-series data efficiently. This package provides a series of helper
// functions that create alternate views of data.

// Any projection that has time as a constituent member of the clustering key will get an additional 
// date-bucket field added to the partition key to distribute records. In addition, the 
package utils

import "github.com/gocql/gocql"

// ClusterConfigGeneratorFn is a functio nthat generates a GoCQL cluster configuration. We
// use this as the GoCQL library stores state on this object and so it shouldn't be re-used.
type ClusterConfigGeneratorFn func() *gocql.ClusterConfig

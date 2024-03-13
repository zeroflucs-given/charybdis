module github.com/zeroflucs-given/charybdis

go 1.21

require (
	github.com/gocql/gocql v1.3.1
	github.com/mitchellh/mapstructure v1.5.0
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/gocqlx/v2 v2.8.0
	github.com/stretchr/testify v1.9.0
	github.com/zeroflucs-given/generics v0.0.0-20240313011108-a343063bef3f
	go.opentelemetry.io/otel v1.24.0
	go.opentelemetry.io/otel/trace v1.24.0
	go.uber.org/zap v1.27.0
	golang.org/x/sync v0.6.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.1 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/hailocab/go-hostpool v0.0.0-20160125115350-e80d13ce29ed // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/otel/metric v1.24.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.7.2

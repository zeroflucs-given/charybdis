module github.com/zeroflucs-given/charybdis

go 1.25.5

tool go.uber.org/mock/mockgen

replace github.com/gocql/gocql => github.com/scylladb/gocql v1.16.1

require (
	github.com/gocql/gocql v1.6.0
	github.com/mitchellh/mapstructure v1.5.0
	github.com/scylladb/go-reflectx v1.0.1
	github.com/scylladb/gocqlx/v2 v2.8.0
	github.com/stretchr/testify v1.11.1
	github.com/zeroflucs-given/generics v0.0.0-20260129235756-dd843c240aba
	go.opentelemetry.io/otel v1.40.0
	go.opentelemetry.io/otel/trace v1.40.0
	go.uber.org/mock v0.6.0
	go.uber.org/zap v1.27.1
	golang.org/x/sync v0.19.0
)

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/go-logr/logr v1.4.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.18.4 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.opentelemetry.io/auto/sdk v1.2.1 // indirect
	go.opentelemetry.io/otel/metric v1.40.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/mod v0.27.0 // indirect
	golang.org/x/tools v0.36.0 // indirect
	gopkg.in/inf.v0 v0.9.1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

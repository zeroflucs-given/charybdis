#!/bin/bash
set -e

# Format all files
echo "Formatting....."
go fmt ./...

# Basic go vetting
echo "Vetting...."
go vet ./...

# More intrusive checks
echo "Linting (Code)...."
golangci-lint run

# Check test coverage
echo "Testing...."
go test -v ./... -cover -coverprofile=coverage.out -coverpkg=./...
go tool cover -html coverage.out -o coverage.html

# Race Testing
echo "Race Testing...."
go test -v ./... -race

# Benchmark Testing
echo "Benchmarks...."
go test -v -bench=. ./...

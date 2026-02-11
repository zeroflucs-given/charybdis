#!/bin/bash
set -e

CONTAINER_RUNNER=${CONTAINER_RUNNER:-podman}
if ! command -v "$CONTAINER_RUNNER" >& /dev/null; then
  CONTAINER_RUNNER=docker
fi

if ! command -v "$CONTAINER_RUNNER" >& /dev/null; then
  echo "No container runner binary found"
  echo "install the command line tools for docker or podman"
  echo "Or if already installed, set the env var CONTAINER_RUNNER to the binary location"
  exit 1
fi

go build ./...
go vet ./...

pushd ./testing
"$CONTAINER_RUNNER" compose pull
"$CONTAINER_RUNNER" compose up -d
popd

echo "Awaiting ScyllaDB: "
while ! nc -z localhost 9042; do
  echo "."
  sleep 0.1
done

go test -v -cover -covermode=atomic -coverprofile=coverage.out -coverpkg=./...  ./...

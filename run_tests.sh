#!/bin/bash
set -e

go build ./...
go vet ./...

pushd ./testing
docker-compose pull
docker-compose up -d

echo "Awaiting ScyllaDB: "
while ! nc -z localhost 9042; do 
  echo "."
  sleep 0.1
done

popd 

go test -v -cover -covermode=atomic -coverprofile=coverage.out -coverpkg=./...  ./...
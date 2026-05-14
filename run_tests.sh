#!/usr/bin/env bash
set -e

CONTAINER_RUNNER=${CONTAINER_RUNNER:-podman}
if ! command -v "$CONTAINER_RUNNER" >& /dev/null; then
  CONTAINER_RUNNER=docker
fi

if ! command -v "$CONTAINER_RUNNER" >& /dev/null; then
  echo "No container runner binary found"
  echo "install the command line tools for docker or podman"
  echo "Or if already installed, ensure they're in your PATH, or set the env var CONTAINER_RUNNER to the binary location"
  exit 1
fi

pushd ./testing/single-node
# "$CONTAINER_RUNNER" compose pull
"$CONTAINER_RUNNER" compose up -d
popd

echo "Awaiting ScyllaDB: "
while ! nc -z localhost 9042; do
  echo "."
  sleep 0.1
done

go build ./...
go vet ./...

RED=$(printf "\e[38;2;240;82;79m")
GREEN=$(printf "\e[38;2;92;150;44m")
CYAN=$(printf "\e[38;2;0;163;163m")
RESET=$(printf "\e[0m")
function highlight() {
  local PATTERN="$1"
  local COLOR="$2"
  shift
  shift
  sed -u -e 's!'"${PATTERN}"'!'"$COLOR"'&'"$RESET"'!g' "$@"
}

go test -v -cover -covermode=atomic -coverprofile=coverage.out -coverpkg=./...  ./... | highlight "PASS" "$GREEN" | highlight "FAIL" "$RED" | highlight "RUN" "$CYAN"

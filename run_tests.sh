#!/usr/bin/env bash
set -e

COMPOSE_RUN=${COMPOSE_RUN:-1}
COMPOSE_STOP=${COMPOSE_STOP:-0}
COMPOSE_TIMEOUT=${COMPOSE_TIMEOUT:-20}

RED=$(printf "\e[38;2;240;82;79m")
GREEN=$(printf "\e[38;2;92;150;44m")
CYAN=$(printf "\e[38;2;0;163;163m")
BLUE=$(printf "\e[38;2;57;147;212m")
RESET=$(printf "\e[0m")

function highlight() {
  local PATTERN="$1"
  local COLOR="$2"
  shift
  shift
  sed -u -e 's!'"${PATTERN}"'!'"$COLOR"'&'"$RESET"'!g' "$@"
}

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

if ! "$CONTAINER_RUNNER" ps >& /dev/null; then
  printf "Unable to talk to your container subsystem '%s'\n" "$CONTAINER_RUNNER"
  printf "Check that it's running, or set the env var \$CONTAINER_RUNNER to 'docker', 'podman', 'container', etc as required.\n"
  exit 2
fi

function header() {
  printf "${BLUE}%s${RESET}\n" "$@"
}

while true; do
    if [[ ! "$1" =~ ^- ]]; then
      break
    fi

    case "$1" in
    -n | --no-compose )
      COMPOSE_RUN=0
      ;;
    -s | --stop-compose )
      COMPOSE_STOP=1
      ;;
    -t | --compose-timeout )
      COMPOSE_TIMEOUT=$2
      shift
      ;;
    * )
      printf "${RED}Unknown option %s (ignored)${RESET}\n" "$1"
      ;;
    esac

    shift
done

if [[ "$COMPOSE_RUN" = "1" ]]; then
  "$CONTAINER_RUNNER" compose -f ./testing/single-node/docker-compose.yml up -d
  printf "Waiting for ScyllaDB: "
  #alt:  "$CONTAINER_RUNNER" exec single-node-scylla-1 cqlsh -u cassandra -p cassandra -e "DESCRIBE SCHEMA"
  while ! nc -z localhost 9042; do
    echo "."
    sleep 0.1
    if [[ "$SECONDS" -gt "$COMPOSE_TIMEOUT" ]]; then
      printf "Timed out waiting for Scylla container to be ready after %d seconds\n" "$SECONDS"
      exit 3
    fi
  done
fi

header "Running 'go vet' to check basic diagnostics"
go vet ./...

header "Running 'go build' to test the library compiles"
go build ./...

header "Running 'go fmt' to ensure basic formatting/linting rules"
go fmt ./...

header "Running tests"
go test -v -cover -covermode=atomic -coverprofile=coverage.out -coverpkg=./...  ./... \
  | highlight "PASS" "$GREEN" \
  | highlight "FAIL" "$RED"   \
  | highlight "RUN" "$CYAN"


if [[ "$COMPOSE_STOP" = "1" ]]; then
  header "Stopping Scylla"
  "$CONTAINER_RUNNER" compose -f ./testing/single-node/docker-compose.yml down
fi

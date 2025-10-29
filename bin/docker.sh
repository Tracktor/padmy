#!/usr/bin/env bash

# DESCRIPTION: A helper script to run spinup the test environement.
# USAGE: ./bin/docker.sh [docker-compose-args]


set -euo pipefail

cd "$(dirname "$0")/.." || exit

docker compose -f tests/docker-compose.yml --project-directory tests "${@}"

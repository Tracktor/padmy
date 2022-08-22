#!/usr/bin/env bash

cd "$(dirname "$0")/.." || exit

docker compose -f tests/docker-compose.yml --project-directory tests "${@}"

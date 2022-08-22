#!/usr/bin/env bash

set -xe

readonly INIT_FILE="${INIT_FILE:=${PGDATA}/init.sql}"
readonly SQL_FOLDER="${SQL_FOLDER:=/sql_scripts}"

for f in $(ls -1v "${SQL_FOLDER}"/*.sql); do
  cat "${f}" >>"${INIT_FILE}"
done

echo "CREATE DATABASE test;" | psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER"
cat "${INIT_FILE}" | psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "test"

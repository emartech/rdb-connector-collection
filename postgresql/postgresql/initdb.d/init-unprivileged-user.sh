#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE TABLE access_denied (id int);
    CREATE USER unprivileged WITH NOSUPERUSER UNENCRYPTED PASSWORD 'unprivilegedpw';
EOSQL

#!/bin/bash
# Target database: same tables, empty. xDB modernized expects target tables to exist.
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /docker-entrypoint-initdb.d/orders-schema.sql.tmpl

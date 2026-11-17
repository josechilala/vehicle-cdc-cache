#!/usr/bin/env bash
set -euo pipefail

database_exists="$(
  psql \
    --set=ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname postgres \
    --set=database_name="$POSTGRES_DB" \
    --tuples-only \
    --no-align \
    --file=- <<'SQL'
SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'database_name');
SQL
)"
if [[ "$database_exists" != "t" ]]; then
  createdb --username "$POSTGRES_USER" "$POSTGRES_DB"
fi

seed_canary="$(
  psql \
    --set=ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --tuples-only \
    --no-align \
    --command="SELECT NOT (
      to_regclass('public.vehicles') IS NOT NULL
      AND to_regclass('public.positions') IS NOT NULL
    )"
)"

psql \
  --set=ON_ERROR_STOP=1 \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set=debezium_password="$DEBEZIUM_PASSWORD" \
  --set=writer_password="$WRITER_PASSWORD" \
  --set=seed_canary="$seed_canary" \
  --file=/bootstrap/init.sql

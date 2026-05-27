#!/bin/bash
# MetroPulse dashboard reader login provisioning and read-only access verification.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
cd "$PROJECT_ROOT"

cleanup_sql_files() {
  docker compose exec -T --user root warehouse-postgres rm -f \
    /tmp/create_dashboard_reader_login.sql /tmp/verify_dashboard_reader_access.sql >/dev/null 2>&1 || true
}
trap cleanup_sql_files EXIT

docker compose cp sql/postgres/create_dashboard_reader_login.sql warehouse-postgres:/tmp/
docker compose cp sql/postgres/verify_dashboard_reader_access.sql warehouse-postgres:/tmp/

docker compose exec -T warehouse-postgres sh -ec '
  : "${WAREHOUSE_DASHBOARD_READER_USER:?Set WAREHOUSE_DASHBOARD_READER_USER in .env}"
  : "${WAREHOUSE_DASHBOARD_READER_PASSWORD:?Set WAREHOUSE_DASHBOARD_READER_PASSWORD in .env}"
  placeholder="CHANGE""_ME_""DASHBOARD_READER_PASSWORD"
  if [ "$WAREHOUSE_DASHBOARD_READER_PASSWORD" = "$placeholder" ]; then
    echo "Replace WAREHOUSE_DASHBOARD_READER_PASSWORD placeholder in .env before provisioning."
    exit 1
  fi

  psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=dashboard_reader_user="$WAREHOUSE_DASHBOARD_READER_USER" \
    --set=dashboard_reader_password="$WAREHOUSE_DASHBOARD_READER_PASSWORD" \
    --file /tmp/create_dashboard_reader_login.sql

  psql -v ON_ERROR_STOP=1 \
    --username "$POSTGRES_USER" \
    --dbname "$POSTGRES_DB" \
    --set=dashboard_reader_user="$WAREHOUSE_DASHBOARD_READER_USER" \
    --file /tmp/verify_dashboard_reader_access.sql

  PGPASSWORD="$WAREHOUSE_DASHBOARD_READER_PASSWORD" psql -v ON_ERROR_STOP=1 \
    --host 127.0.0.1 \
    --username "$WAREHOUSE_DASHBOARD_READER_USER" \
    --dbname "$POSTGRES_DB" \
    --command "SHOW timezone;" \
    --command "SELECT COUNT(*) AS readable_hourly_kpi_rows FROM mart.dashboard_hourly_demand_kpi;"
'

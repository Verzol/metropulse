-- Validate dashboard consumer privileges without performing writes.

\set ON_ERROR_STOP on

SELECT rolname, rolcanlogin
FROM pg_roles
WHERE rolname = :'dashboard_reader_user';

SELECT
    pg_has_role(:'dashboard_reader_user', 'dashboard_reader', 'member') AS inherits_dashboard_reader,
    has_schema_privilege(:'dashboard_reader_user', 'mart', 'USAGE') AS can_use_mart_schema,
    has_schema_privilege(:'dashboard_reader_user', 'mart', 'CREATE') AS can_create_in_mart_schema,
    has_schema_privilege(:'dashboard_reader_user', 'staging', 'USAGE') AS can_use_staging_schema,
    has_table_privilege(:'dashboard_reader_user', 'mart.dashboard_hourly_demand_kpi', 'SELECT') AS can_select_hourly_kpi,
    has_table_privilege(:'dashboard_reader_user', 'mart.dashboard_zone_summary', 'SELECT') AS can_select_zone_summary,
    has_table_privilege(:'dashboard_reader_user', 'mart.dashboard_payment_tip_summary', 'SELECT') AS can_select_tip_summary,
    has_table_privilege(:'dashboard_reader_user', 'mart.dashboard_hourly_demand_kpi', 'INSERT') AS can_insert_hourly_kpi,
    has_table_privilege(:'dashboard_reader_user', 'mart.dashboard_hourly_demand_kpi', 'UPDATE') AS can_update_hourly_kpi,
    has_table_privilege(:'dashboard_reader_user', 'mart.dashboard_hourly_demand_kpi', 'DELETE') AS can_delete_hourly_kpi;

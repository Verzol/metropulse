from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_ROOT = os.environ.get("METROPULSE_PROJECT_ROOT", "/opt/airflow/metropulse")

DEFAULT_ARGS = {
    "owner": "metropulse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def project_command(command: str) -> str:
    return f"set -euo pipefail; cd {PROJECT_ROOT}; {command} "


with DAG(
    dag_id="metropulse_gold_pipeline",
    description="Orchestrate Gold processing and PostgreSQL serving publication validation.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["metropulse", "gold", "spark", "minio", "postgresql", "ml", "dashboard"],
) as dag:
    start = EmptyOperator(task_id="start")

    validate_project_root = BashOperator(
        task_id="validate_project_root",
        bash_command=project_command(
            "test -f docker-compose.yml && "
            "test -f src/processing/gold_transform.py && "
            "test -f src/quality/gold_quality_check.py && "
            "test -f src/processing/gold_dashboard_marts.py && "
            "test -f src/serving/publish_gold_to_postgres.py && "
            "test -f src/serving/publish_gold_fare_tip_to_postgres.py && "
            "test -f src/serving/publish_dashboard_to_postgres.py && "
            "test -f src/quality/postgres_warehouse_quality_check.py && "
            "test -f src/quality/postgres_fare_tip_quality_check.py && "
            "test -f src/quality/postgres_dashboard_quality_check.py && "
            "test -f scripts/run_gold_docker.sh && "
            "test -f scripts/run_gold_quality_docker.sh && "
            "test -f scripts/run_gold_dashboard_docker.sh && "
            "test -f scripts/run_gold_postgres_publish_docker.sh && "
            "test -f scripts/run_postgres_warehouse_quality_docker.sh && "
            "test -f scripts/run_fare_tip_postgres_publish_docker.sh && "
            "test -f scripts/run_postgres_fare_tip_quality_docker.sh && "
            "test -f scripts/run_dashboard_postgres_publish_docker.sh && "
            "test -f scripts/run_postgres_dashboard_quality_docker.sh && "
            "test -f sql/postgres/init_warehouse.sql && "
            "test -f sql/postgres/promote_gold_demand_features.sql && "
            "test -f sql/postgres/promote_gold_fare_tip_features.sql && "
            "test -f sql/postgres/promote_dashboard_marts.sql"
        ),
    )

    validate_docker_access = BashOperator(
        task_id="validate_docker_access",
        bash_command=project_command("docker compose version && docker compose ps"),
    )

    check_required_services = BashOperator(
        task_id="check_required_services",
        bash_command=project_command(
            "running_services=$(docker compose ps --services --filter status=running); "
            "for service in minio spark-master spark-worker-1 spark-worker-2 warehouse-postgres; do "
            "echo \"$running_services\" | grep -qx \"$service\" || "
            "(echo \"Missing running service: $service\" && exit 1); "
            "done"
        ),
    )

    run_gold_transform = BashOperator(
        task_id="run_gold_transform",
        bash_command=project_command("./scripts/run_gold_docker.sh"),
        execution_timeout=timedelta(hours=4),
    )

    run_gold_quality_check = BashOperator(
        task_id="run_gold_quality_check",
        bash_command=project_command("./scripts/run_gold_quality_docker.sh"),
        execution_timeout=timedelta(hours=2),
    )

    run_gold_dashboard_marts = BashOperator(
        task_id="run_gold_dashboard_marts",
        bash_command=project_command("./scripts/run_gold_dashboard_docker.sh"),
        execution_timeout=timedelta(hours=2),
    )

    initialize_warehouse = BashOperator(
        task_id="initialize_warehouse",
        bash_command=project_command(
            "docker compose exec -T warehouse-postgres sh -c "
            "'psql -v ON_ERROR_STOP=1 --username \"$POSTGRES_USER\" "
            "--dbname \"$POSTGRES_DB\" "
            "--file /docker-entrypoint-initdb.d/001_init_warehouse.sql'"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    publish_gold_demand_to_postgres = BashOperator(
        task_id="publish_gold_demand_to_postgres",
        bash_command=project_command("./scripts/run_gold_postgres_publish_docker.sh"),
        execution_timeout=timedelta(hours=2),
    )

    validate_postgres_warehouse_publication = BashOperator(
        task_id="validate_postgres_warehouse_publication",
        bash_command=project_command("./scripts/run_postgres_warehouse_quality_docker.sh"),
        execution_timeout=timedelta(hours=2),
    )

    publish_gold_fare_tip_to_postgres = BashOperator(
        task_id="publish_gold_fare_tip_to_postgres",
        bash_command=project_command("./scripts/run_fare_tip_postgres_publish_docker.sh"),
        execution_timeout=timedelta(hours=8),
    )

    validate_postgres_fare_tip_publication = BashOperator(
        task_id="validate_postgres_fare_tip_publication",
        bash_command=project_command("./scripts/run_postgres_fare_tip_quality_docker.sh"),
        execution_timeout=timedelta(hours=4),
    )

    publish_dashboard_marts_to_postgres = BashOperator(
        task_id="publish_dashboard_marts_to_postgres",
        bash_command=project_command("./scripts/run_dashboard_postgres_publish_docker.sh"),
        execution_timeout=timedelta(hours=2),
    )

    validate_postgres_dashboard_publication = BashOperator(
        task_id="validate_postgres_dashboard_publication",
        bash_command=project_command("./scripts/run_postgres_dashboard_quality_docker.sh"),
        execution_timeout=timedelta(hours=2),
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> validate_project_root
        >> validate_docker_access
        >> check_required_services
        >> run_gold_transform
        >> run_gold_quality_check
        >> run_gold_dashboard_marts
        >> initialize_warehouse
        >> publish_gold_demand_to_postgres
        >> validate_postgres_warehouse_publication
        >> publish_gold_fare_tip_to_postgres
        >> validate_postgres_fare_tip_publication
        >> publish_dashboard_marts_to_postgres
        >> validate_postgres_dashboard_publication
        >> finish
    )

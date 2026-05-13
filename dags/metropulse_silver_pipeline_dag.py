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
    dag_id="metropulse_silver_pipeline",
    description="Orchestrate MetroPulse Silver and Silver Core Spark jobs.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["metropulse", "silver", "spark", "minio"],
) as dag:
    start = EmptyOperator(task_id="start")

    validate_project_root = BashOperator(
        task_id="validate_project_root",
        bash_command=project_command(
            "test -f docker-compose.yml && "
            "test -f scripts/run_silver_docker.sh && "
            "test -f scripts/run_silver_core_docker.sh && "
            "test -f scripts/run_silver_quality_docker.sh && "
            "test -f scripts/run_silver_clean_docker.sh"
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
            "for service in minio spark-master spark-worker-1 spark-worker-2; do "
            "echo \"$running_services\" | grep -qx \"$service\" || "
            "(echo \"Missing running service: $service\" && exit 1); "
            "done"
        ),
    )

    run_silver_transform = BashOperator(
        task_id="run_silver_transform",
        bash_command=project_command("./scripts/run_silver_docker.sh"),
        execution_timeout=timedelta(hours=6),
    )

    run_silver_core_transform = BashOperator(
        task_id="run_silver_core_transform",
        bash_command=project_command("./scripts/run_silver_core_docker.sh"),
        execution_timeout=timedelta(hours=4),
    )

    run_silver_quality_check = BashOperator(
        task_id="run_silver_quality_check",
        bash_command=project_command("./scripts/run_silver_quality_docker.sh"),
        execution_timeout=timedelta(hours=3),
    )

    run_silver_clean_transform = BashOperator(
        task_id="run_silver_clean_transform",
        bash_command=project_command("./scripts/run_silver_clean_docker.sh"),
        execution_timeout=timedelta(hours=4),
    )

    finish = EmptyOperator(task_id="finish")

    (
        start
        >> validate_project_root
        >> validate_docker_access
        >> check_required_services
        >> run_silver_transform
        >> run_silver_core_transform
        >> run_silver_quality_check
        >> run_silver_clean_transform
        >> finish
    )

.PHONY: help start stop restart logs producer bronze silver silver-core silver-quality silver-clean gold gold-quality gold-dashboard gold-publish-ml gold-publish-dashboard gold-publish-serving warehouse-quality dashboard-warehouse-quality weather clean venv install airflow-init airflow-up airflow-down airflow-logs airflow-dags warehouse-up warehouse-init warehouse-status warehouse-ml-access warehouse-dashboard-access pgadmin-up pgadmin-logs

# Default target
help:
	@echo "MetroPulse - Data Pipeline Commands"
	@echo "===================================="
	@echo ""
	@echo "Setup:"
	@echo "  make venv          - Create Python virtual environment"
	@echo "  make install       - Install Python dependencies"
	@echo ""
	@echo "Docker & Services:"
	@echo "  make start         - Start all Docker services (Kafka, MinIO, Spark, Airflow, etc.)"
	@echo "  make stop          - Stop all Docker services"
	@echo "  make restart       - Restart all services"
	@echo "  make logs          - View Docker service logs"
	@echo "  make airflow-init  - Initialize Airflow metadata DB and admin user"
	@echo "  make airflow-up    - Start Airflow webserver and scheduler"
	@echo "  make airflow-logs  - View Airflow logs"
	@echo "  make warehouse-up  - Start PostgreSQL Data Warehouse service"
	@echo "  make warehouse-init - Initialize warehouse schemas and serving tables"
	@echo "  make warehouse-status - Check warehouse schemas and tables"
	@echo "  make warehouse-ml-access - Provision and verify read-only ML login"
	@echo "  make warehouse-dashboard-access - Provision and verify read-only dashboard login"
	@echo "  make pgadmin-up    - Start pgAdmin UI for Warehouse inspection"
	@echo "  make pgadmin-logs  - View pgAdmin logs"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make producer      - Stream NYC taxi data to Kafka"
	@echo "  make weather       - Stream NYC weather data (Open-Meteo API) to Kafka"
	@echo "  make bronze        - Ingest Kafka → MinIO (Bronze layer)"
	@echo "  make silver        - Build Silver taxi-weather parquet from Bronze"
	@echo "  make silver-core   - Build compact Silver core parquet from existing Silver"
	@echo "  make silver-quality - Run Silver Core data quality checks"
	@echo "  make silver-clean  - Build cleaned Silver dataset from Silver Core"
	@echo "  make gold          - Build Gold ML-ready parquet datasets from Silver Core"
	@echo "  make gold-quality  - Run Gold data quality checks"
	@echo "  make gold-dashboard - Build Gold aggregate dashboard marts"
	@echo "  make gold-publish-ml - Publish Gold demand features to PostgreSQL and validate"
	@echo "  make gold-publish-dashboard - Publish Gold dashboard marts to PostgreSQL and validate"
	@echo "  make gold-publish-serving - Publish and validate ML plus dashboard serving tables"
	@echo "  make warehouse-quality - Re-run pending PostgreSQL publication validation"
	@echo "  make dashboard-warehouse-quality - Re-run pending dashboard publication validation"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         - Stop services (keep data)"
	@echo "  make clean-all     - Stop services & remove all data"
	@echo ""
	@echo "URLs (Local/Docker):"
	@echo "  Kafka:       localhost:9092"
	@echo "  MinIO:       http://localhost:9001 (credentials from .env)"
	@echo "  Spark:       http://localhost:8080"
	@echo "  Airflow:     http://localhost:8088 (default admin from .env)"
	@echo "  Warehouse:   localhost:5433 (credentials from .env)"
	@echo "  pgAdmin:     http://localhost:5050 (credentials from .env)"
	@echo ""
	@echo "URLs (GCP VM):"
	@echo "  MinIO:       http://<VM_EXTERNAL_IP>:9001"
	@echo "  Spark:       http://<VM_EXTERNAL_IP>:8080"
	@echo "  Airflow:     http://<VM_EXTERNAL_IP>:8088"
	@echo ""
	@echo "Examples:"
	@echo "  # Full pipeline:"
	@echo "  make start && make weather && make producer && make bronze && make silver"
	@echo ""
	@echo "  # Just weather data:"
	@echo "  make start && make weather"


# Setup targets
venv:
	python3 -m venv .venv
	@echo "Virtual environment created. Activate with: source .venv/bin/activate"

install: venv
	. .venv/bin/activate && pip install --upgrade pip -q && pip install -r requirements.txt -q
	@echo "Dependencies installed!"

# Docker targets
start:
	@echo "Starting Docker services..."
	docker compose up -d
	@echo "Services starting (may take 30-60 seconds)..."
	@sleep 5
	docker compose ps
	@echo ""
	@echo "URLs:"
	@echo "  GCP VM:"
	@echo "    MinIO:  http://<VM_EXTERNAL_IP>:9001"
	@echo "    Spark:  http://<VM_EXTERNAL_IP>:8080"
	@echo "    Airflow: http://<VM_EXTERNAL_IP>:8088"
	@echo ""
	@echo "  Local/SSH Tunnel:"
	@echo "    MinIO:  http://localhost:9001"
	@echo "    Spark:  http://localhost:8080"
	@echo "    Airflow: http://localhost:8088"
	@echo "    Warehouse: 127.0.0.1:5433"
	@echo "    pgAdmin: http://localhost:5050"

stop:
	@echo "Stopping Docker services..."
	docker compose stop
	@echo "Services stopped (data preserved)"

restart: stop start
	@echo "Services restarted"

logs:
	docker compose logs -f

airflow-init:
	@echo "Initializing Airflow metadata DB and admin user..."
	docker compose up airflow-init

airflow-up:
	@echo "Starting Airflow webserver and scheduler..."
	docker compose up -d airflow-webserver airflow-scheduler
	@echo "Airflow UI: http://localhost:8088"

airflow-down:
	@echo "Stopping Airflow webserver and scheduler..."
	docker compose stop airflow-webserver airflow-scheduler

airflow-logs:
	docker compose logs -f airflow-webserver airflow-scheduler

airflow-dags:
	docker compose exec airflow-webserver airflow dags list

warehouse-up:
	@echo "Starting PostgreSQL Data Warehouse service..."
	docker compose up -d warehouse-postgres

warehouse-init: warehouse-up
	@echo "Waiting for PostgreSQL Data Warehouse readiness..."
	@attempt=0; \
	until docker compose exec -T warehouse-postgres sh -c 'pg_isready --username "$$POSTGRES_USER" --dbname "$$POSTGRES_DB"' >/dev/null 2>&1; do \
		attempt=$$((attempt + 1)); \
		if [ "$$attempt" -ge 30 ]; then \
			echo "Warehouse PostgreSQL did not become ready in time."; \
			exit 1; \
		fi; \
		sleep 2; \
	done
	@echo "Initializing PostgreSQL Data Warehouse schemas and tables..."
	docker compose exec -T warehouse-postgres sh -c 'psql -v ON_ERROR_STOP=1 --username "$$POSTGRES_USER" --dbname "$$POSTGRES_DB" --file /docker-entrypoint-initdb.d/001_init_warehouse.sql'

warehouse-status:
	@echo "Checking PostgreSQL Data Warehouse schemas and tables..."
	docker compose exec -T warehouse-postgres sh -c 'psql -v ON_ERROR_STOP=1 --username "$$POSTGRES_USER" --dbname "$$POSTGRES_DB" --command "SHOW timezone;" --command "\dn ml" --command "\dn mart" --command "\dn audit" --command "\dn staging" --command "\dt ml.*" --command "\dt mart.*" --command "\dt audit.*" --command "SELECT COUNT(*) AS gold_demand_features_rows FROM ml.gold_demand_features;" --command "SELECT COUNT(*) AS dashboard_hourly_rows FROM mart.dashboard_hourly_demand_kpi;" --command "SELECT COUNT(*) AS dashboard_zone_rows FROM mart.dashboard_zone_summary;" --command "SELECT COUNT(*) AS dashboard_payment_rows FROM mart.dashboard_payment_tip_summary;"'

warehouse-ml-access: warehouse-init
	@echo "Provisioning read-only PostgreSQL login for ML consumers..."
	./scripts/setup_ml_reader_access_docker.sh

warehouse-dashboard-access: warehouse-init
	@echo "Provisioning read-only PostgreSQL login for dashboard consumers..."
	./scripts/setup_dashboard_reader_access_docker.sh

pgadmin-up: warehouse-up
	@echo "Starting pgAdmin UI for PostgreSQL Warehouse..."
	docker compose up -d pgadmin
	@echo "pgAdmin UI: http://localhost:5050 (use SSH tunnel when accessing from a personal machine)"

pgadmin-logs:
	docker compose logs -f pgadmin

clean: stop
	@echo "Services stopped. Data preserved."

clean-all:
	@echo "WARNING: This will delete MinIO, Airflow Postgres, Warehouse Postgres, and pgAdmin configuration volume data!"
	@read -p "Continue? (y/N) " confirm && [ "$$confirm" = "y" ] && docker compose down -v || echo "Cancelled"

# Pipeline targets
producer:
	@echo "Starting data producer..."
	. .venv/bin/activate && python3 src/ingestion/producer.py

weather:
	@echo "Starting weather data producer (Open-Meteo)..."
	. .venv/bin/activate && python3 src/ingestion/weather_openmeteo_producer.py

bronze:
	@echo "Starting Bronze layer ingestion..."
	./scripts/run_bronze_docker.sh

silver:
	@echo "Starting Silver layer enrichment..."
	./scripts/run_silver_docker.sh

silver-core:
	@echo "Starting Silver core build..."
	./scripts/run_silver_core_docker.sh

silver-quality:
	@echo "Starting Silver quality checks..."
	./scripts/run_silver_quality_docker.sh

silver-clean:
	@echo "Starting Silver clean build..."
	./scripts/run_silver_clean_docker.sh

gold:
	@echo "Starting Gold layer build..."
	./scripts/run_gold_docker.sh

gold-quality:
	@echo "Starting Gold quality checks..."
	./scripts/run_gold_quality_docker.sh

gold-dashboard:
	@echo "Starting Gold dashboard marts build..."
	./scripts/run_gold_dashboard_docker.sh

gold-publish-ml: warehouse-init
	@echo "Publishing Gold demand features to PostgreSQL Warehouse..."
	./scripts/run_gold_postgres_publish_docker.sh
	./scripts/run_postgres_warehouse_quality_docker.sh

gold-publish-dashboard: warehouse-init
	@echo "Publishing Gold dashboard marts to PostgreSQL Warehouse..."
	./scripts/run_dashboard_postgres_publish_docker.sh
	./scripts/run_postgres_dashboard_quality_docker.sh

gold-publish-serving: gold-publish-ml gold-publish-dashboard
	@echo "ML and dashboard serving tables published and validated."

warehouse-quality:
	@echo "Validating pending PostgreSQL Warehouse publication..."
	./scripts/run_postgres_warehouse_quality_docker.sh

dashboard-warehouse-quality:
	@echo "Validating pending PostgreSQL dashboard publication..."
	./scripts/run_postgres_dashboard_quality_docker.sh

# Status check
status:
	@echo "Service Status:"
	@docker compose ps
	@echo ""
	@echo "Kafka connection:"
	@python3 -c "import socket; s=socket.socket(); r=s.connect_ex(('localhost', 9092)); s.close(); print('OK' if r==0 else 'FAILED')" || echo "FAILED"

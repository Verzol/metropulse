.PHONY: help start stop restart logs producer bronze weather clean venv install

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
	@echo "  make start         - Start all Docker services (Kafka, MinIO, Spark, etc.)"
	@echo "  make stop          - Stop all Docker services"
	@echo "  make restart       - Restart all services"
	@echo "  make logs          - View Docker service logs"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  make producer      - Stream NYC taxi data to Kafka"
	@echo "  make weather       - Stream NYC weather data (Open-Meteo API) to Kafka"
	@echo "  make bronze        - Ingest Kafka → MinIO (Bronze layer)"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean         - Stop services (keep data)"
	@echo "  make clean-all     - Stop services & remove all data"
	@echo ""
	@echo "URLs (Local/Docker):"
	@echo "  Kafka:       localhost:9092"
	@echo "  MinIO:       http://localhost:9001 (admin/metropulse2026)"
	@echo "  Spark:       http://localhost:8080"
	@echo ""
	@echo "URLs (GCP VM - 34.21.193.160):"
	@echo "  MinIO:       http://34.21.193.160:9001 (admin/metropulse2026)"
	@echo "  Spark:       http://34.21.193.160:8080"
	@echo ""
	@echo "Examples:"
	@echo "  # Full pipeline:"
	@echo "  make start && make weather && make producer && make bronze"
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
	@echo "  GCP (34.21.193.160):"
	@echo "    MinIO:  http://34.21.193.160:9001"
	@echo "    Spark:  http://34.21.193.160:8080"
	@echo ""
	@echo "  Local/SSH Tunnel:"
	@echo "    MinIO:  http://localhost:9001"
	@echo "    Spark:  http://localhost:8080"

stop:
	@echo "Stopping Docker services..."
	docker compose stop
	@echo "Services stopped (data preserved)"

restart: stop start
	@echo "Services restarted"

logs:
	docker compose logs -f

clean: stop
	@echo "Services stopped. Data preserved."

clean-all:
	@echo "WARNING: This will delete all MinIO data!"
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

# Status check
status:
	@echo "Service Status:"
	@docker compose ps
	@echo ""
	@echo "Kafka connection:"
	@python3 -c "import socket; s=socket.socket(); r=s.connect_ex(('localhost', 9092)); s.close(); print('OK' if r==0 else 'FAILED')" || echo "FAILED"

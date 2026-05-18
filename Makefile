.PHONY: build start stop restart logs clean pipeline dbt-test status help

COMPOSE = docker compose
DAG_ID  = fraud_lakehouse_pipeline

help:
	@echo ""
	@echo "  Data Lakehouse — Fraud Detection"
	@echo "  ─────────────────────────────────────────────────────────────────"
	@echo "  make build       Build / rebuild Docker images (downloads JARs)"
	@echo "  make start       Start all services"
	@echo "  make stop        Stop all services"
	@echo "  make restart     Rebuild and restart everything"
	@echo "  make status      Show running containers"
	@echo "  make logs        Tail all logs"
	@echo "  make pipeline    Trigger the Airflow ETL DAG manually"
	@echo "  make dbt-test    Run dbt quality tests standalone"
	@echo "  make clean       Stop services and remove all volumes (destructive)"
	@echo "  make urls        Print all service URLs and credentials"
	@echo ""

build:
	$(COMPOSE) build --no-cache

start:
	$(COMPOSE) up -d
	@echo ""
	@echo "  Services starting… run 'make logs' to follow progress."
	@echo "  Run 'make urls' for all endpoints."

stop:
	$(COMPOSE) down

restart: stop build start

status:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

pipeline:
	@echo "Triggering DAG: $(DAG_ID)"
	$(COMPOSE) exec airflow-webserver \
		airflow dags trigger $(DAG_ID)

dbt-test:
	$(COMPOSE) exec airflow-webserver bash -c "\
		cd /opt/dbt && \
		dbt deps --profiles-dir /opt/dbt && \
		dbt test --profiles-dir /opt/dbt --target dev"

clean:
	@echo "WARNING: This removes all Docker volumes (all data will be lost)."
	@read -p "Continue? [y/N] " c; [ "$$c" = "y" ] || exit 1
	$(COMPOSE) down -v --remove-orphans

urls:
	@echo ""
	@echo "  ┌─────────────────────────────────────────────────────────────┐"
	@echo "  │  Service           URL                        Credentials   │"
	@echo "  ├─────────────────────────────────────────────────────────────┤"
	@echo "  │  MinIO Console     http://localhost:9001      admin / password123    │"
	@echo "  │  Nessie API        http://localhost:19120/api/v1             │"
	@echo "  │  Spark Master UI   http://localhost:8080                     │"
	@echo "  │  Airflow UI        http://localhost:8082      admin / admin123       │"
	@echo "  │  Dremio UI         http://localhost:9047      admin / Admin1234!     │"
	@echo "  │  Superset UI       http://localhost:8088      admin / admin123       │"
	@echo "  └─────────────────────────────────────────────────────────────┘"
	@echo ""

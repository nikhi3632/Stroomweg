# Stroomweg Makefile

.PHONY: help install migrate db-status db-delete db-reset db-shell db-count ingest ingest-start ingest-stop ingest-logs ingest-status api api-start api-stop api-logs test clean

help:
	@echo "Stroomweg Commands"
	@echo "=================="
	@echo ""
	@echo "Setup:"
	@echo "  make install        Install dependencies"
	@echo ""
	@echo "Database:"
	@echo "  make migrate        Run pending migrations"
	@echo "  make db-status      Show migration status"
	@echo "  make db-delete      Drop all tables (DESTRUCTIVE)"
	@echo "  make db-reset       Drop and recreate all tables (DESTRUCTIVE)"
	@echo "  make db-shell       Open psql shell to database"
	@echo "  make db-count       Show row counts"
	@echo ""
	@echo "Ingest (Railway):"
	@echo "  make ingest         Stop, reset DB, and start ingest"
	@echo "  make ingest-start   Start ingest on Railway"
	@echo "  make ingest-stop    Stop ingest on Railway"
	@echo "  make ingest-logs    View Railway logs"
	@echo "  make ingest-status  Show Railway status"
	@echo ""
	@echo "API (Railway):"
	@echo "  make api            Start API locally (dev)"
	@echo "  make api-start      Start API on Railway"
	@echo "  make api-stop       Stop API on Railway"
	@echo "  make api-logs       View API Railway logs"
	@echo ""
	@echo "Development:"
	@echo "  make test           Run tests"
	@echo "  make clean          Remove cache files"

# Setup
install:
	.venv/bin/pip install -r requirements.txt

# Database
migrate:
	.venv/bin/python scripts/migrate.py

db-status:
	.venv/bin/python scripts/migrate.py --status

db-delete:
	.venv/bin/python scripts/db_delete.py

db-reset:
	.venv/bin/python scripts/db_reset.py

db-shell:
	psql "$$(.venv/bin/python scripts/db_url.py)"

db-count:
	.venv/bin/python scripts/db_count.py

# Ingest (Railway)
ingest:
	railway down --service Stroomweg -y || true
	echo "y" | .venv/bin/python scripts/db_reset.py
	railway up --service Stroomweg --detach

ingest-start:
	railway up --service Stroomweg --detach

ingest-stop:
	railway down --service Stroomweg -y

ingest-logs:
	railway logs --service Stroomweg

ingest-status:
	.venv/bin/python scripts/ingest_status.py

# API
api:
	.venv/bin/uvicorn api.app:app --reload --port 8000

api-start:
	railway up --service Stroomweg-API --detach

api-stop:
	railway down --service Stroomweg-API -y

api-logs:
	railway logs --service Stroomweg-API

# Development
test:
	.venv/bin/python -m pytest tests/ -v

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true

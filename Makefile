.PHONY: help dev build test lint up down clean

help:
	@echo "HiveFlow Lakehouse Platform — Developer Commands:"
	@echo "  make up       - Start complete platform via Docker Compose"
	@echo "  make down     - Stop and tear down all platform containers"
	@echo "  make dev      - Start FastAPI backend and Vite frontend locally"
	@echo "  make test     - Run automated pytest regression suite"
	@echo "  make build    - Compile and bundle React frontend"
	@echo "  make clean    - Remove build caches, .pyc files, and test caches"

up:
	docker compose up -d

down:
	docker compose down

dev:
	uvicorn api.main:app --host 0.0.0.0 --port 8000 --reload &
	cd frontend && npm run dev

test:
	pytest tests/ -v

build:
	cd frontend && npm install && npm run build

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	rm -rf .pytest_cache frontend/dist frontend/node_modules

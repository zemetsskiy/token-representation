# Token Representation Docker Makefile
# Usage: make [target]

.PHONY: help build up down restart logs shell clean rebuild

# Default target
help:
	@echo "Token Representation Docker Commands"
	@echo "====================================="
	@echo ""
	@echo "Available targets:"
	@echo "  make build      - Build the Docker image"
	@echo "  make up         - Start services in foreground"
	@echo "  make up-d       - Start services in background (detached)"
	@echo "  make down       - Stop and remove containers"
	@echo "  make restart    - Restart services"
	@echo "  make logs       - Show logs (follow mode)"
	@echo "  make logs-tail  - Show last 100 lines of logs"
	@echo "  make shell      - Open bash shell in container"
	@echo "  make status     - Show container status"
	@echo "  make clean      - Remove containers, volumes, and images"
	@echo "  make rebuild    - Rebuild and restart services"
	@echo "  make stats      - Show resource usage statistics"
	@echo ""

# Build Docker image
build:
	docker-compose build

# Start services in foreground
up:
	docker-compose up

# Start services in background
up-d:
	docker-compose up -d

# Stop and remove containers
down:
	docker-compose down

# Restart services
restart:
	docker-compose restart

# Show logs (follow mode)
logs:
	docker-compose logs -f

# Show last 100 lines of logs
logs-tail:
	docker-compose logs --tail=100

# Open bash shell in container
shell:
	docker-compose exec token-representation bash

# Show container status
status:
	docker-compose ps

# Remove everything (containers, volumes, images)
clean:
	docker-compose down -v --rmi all

# Rebuild and restart
rebuild:
	docker-compose up -d --build --force-recreate

# Show resource usage
stats:
	docker stats token-representation-worker

# Check if .env file exists
check-env:
	@if [ ! -f ../.env ]; then \
		echo "Error: .env file not found in parent directory!"; \
		echo "Please copy .env.example to ../.env and configure it."; \
		exit 1; \
	fi
	@echo ".env file found ✓"

# Full setup (check env and build)
setup: check-env build
	@echo "Setup complete! Run 'make up' to start the service."

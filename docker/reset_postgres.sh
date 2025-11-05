#!/bin/bash
# Reset PostgreSQL container and data with fixed init.sql
# Run from: docker/ directory

set -e

echo "=========================================="
echo "Resetting PostgreSQL with fixed schema"
echo "=========================================="
echo

# Stop containers
echo "1. Stopping containers..."
docker compose -f docker-compose.full.yml down

# Remove PostgreSQL volume
echo "2. Removing PostgreSQL volume..."
docker volume rm docker_postgres_data 2>/dev/null || echo "   Volume already removed"

# Rebuild and start
echo "3. Rebuilding and starting containers..."
docker compose -f docker-compose.full.yml up -d --build

echo
echo "=========================================="
echo "✅ Done!"
echo "=========================================="
echo
echo "Checking status:"
docker compose -f docker-compose.full.yml ps
echo
echo "Checking PostgreSQL logs:"
docker compose -f docker-compose.full.yml logs postgres | tail -20
echo
echo "To view full logs:"
echo "  docker compose -f docker-compose.full.yml logs -f"
echo
echo "To connect to PostgreSQL:"
echo "  docker exec -it token-metrics-postgres psql -U postgres -d token_metrics"
echo

#!/bin/bash
# Quick fix script for PostgreSQL connection issue

set -e

echo "=========================================="
echo "PostgreSQL Connection Fix"
echo "=========================================="
echo ""

# Check .env file exists
if [ ! -f .env ]; then
    echo "❌ ERROR: .env file not found!"
    echo "Please create .env file with POSTGRES_PASSWORD"
    exit 1
fi

# Check POSTGRES_PASSWORD is set
if ! grep -q "POSTGRES_PASSWORD=" .env; then
    echo "❌ ERROR: POSTGRES_PASSWORD not found in .env"
    echo "Please add: POSTGRES_PASSWORD=your_password"
    exit 1
fi

echo "✅ .env file found with POSTGRES_PASSWORD"
echo ""

# Sync .env to docker/.env
echo "Syncing PostgreSQL settings to docker/.env..."
./sync_env.sh
echo ""

# Step 1: Stop and remove old containers
echo "Step 1: Stopping and removing old PostgreSQL container..."
docker compose -f docker/docker-compose.full.yml down -v 2>/dev/null || true
echo "✅ Old container removed"
echo ""

# Step 2: Start PostgreSQL
echo "Step 2: Starting fresh PostgreSQL container..."
docker compose -f docker/docker-compose.full.yml up postgres -d
echo "✅ PostgreSQL starting..."
echo ""

# Step 3: Wait for PostgreSQL to be ready
echo "Step 3: Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
    if docker exec token-metrics-postgres pg_isready -U postgres >/dev/null 2>&1; then
        echo "✅ PostgreSQL is ready!"
        break
    fi
    echo -n "."
    sleep 1
done
echo ""

# Step 4: Test connection
echo "Step 4: Testing connection..."
if python3 test_postgres_connection.py; then
    echo ""
    echo "=========================================="
    echo "✅ SUCCESS! PostgreSQL is ready to use"
    echo "=========================================="
    echo ""
    echo "You can now run:"
    echo "  python3 worker_scheduled.py --view sol_500_swaps_7_days"
    echo ""
else
    echo ""
    echo "=========================================="
    echo "❌ Connection test failed"
    echo "=========================================="
    echo ""
    echo "Please check:"
    echo "  1. Docker logs: docker logs token-metrics-postgres"
    echo "  2. Connection settings in .env"
    echo "  3. Read POSTGRES_CONNECTION_FIX.md for details"
    echo ""
    exit 1
fi

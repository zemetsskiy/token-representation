#!/bin/bash
# Sync PostgreSQL settings from root .env to docker/.env

set -e

echo "Syncing PostgreSQL settings to docker/.env..."

# Check if root .env exists
if [ ! -f .env ]; then
    echo "❌ ERROR: .env file not found in root directory!"
    exit 1
fi

# Extract PostgreSQL variables from root .env
POSTGRES_VARS=$(grep "^POSTGRES_" .env || true)

if [ -z "$POSTGRES_VARS" ]; then
    echo "❌ ERROR: No POSTGRES_ variables found in .env"
    exit 1
fi

# Create docker/.env with header
cat > docker/.env << 'EOF'
# PostgreSQL configuration for docker-compose
# This file is automatically synced from root .env - DO NOT EDIT MANUALLY
# Run ./sync_env.sh to update

EOF

# Append PostgreSQL variables
echo "$POSTGRES_VARS" >> docker/.env

echo "✅ PostgreSQL settings synced to docker/.env"
echo ""
echo "Synced variables:"
echo "$POSTGRES_VARS"

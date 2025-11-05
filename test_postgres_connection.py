#!/usr/bin/env python3
"""
Test PostgreSQL connection
Quick script to verify PostgreSQL connection settings
"""

import sys
import os
from pathlib import Path

# Setup paths
project_root = Path(__file__).parent
src_path = project_root / 'src'
sys.path.insert(0, str(src_path))

from src.config import Config

def test_connection():
    """Test PostgreSQL connection."""
    print("=" * 80)
    print("PostgreSQL Connection Test")
    print("=" * 80)

    # Show configuration
    print("\nConfiguration:")
    print(f"  Host: {Config.POSTGRES_HOST}")
    print(f"  Port: {Config.POSTGRES_PORT}")
    print(f"  Database: {Config.POSTGRES_DATABASE}")
    print(f"  User: {Config.POSTGRES_USER}")
    print(f"  Password: {'*' * len(Config.POSTGRES_PASSWORD)}")

    print("\nTrying to connect...")

    try:
        from src.database import get_postgres_client

        pg = get_postgres_client()
        print("✅ Connection successful!")

        # Test query
        count = pg.get_metrics_count()
        print(f"\n📊 Database stats:")
        print(f"  Total records: {count:,}")

        # Test simple query
        result = pg.execute_query("SELECT version()")
        print(f"  PostgreSQL version: {result[0]['version'][:50]}...")

        pg.close()
        print("\n✅ All tests passed!")
        return 0

    except Exception as e:
        print(f"\n❌ Connection failed!")
        print(f"Error: {e}")
        print("\n" + "=" * 80)
        print("Troubleshooting:")
        print("=" * 80)
        print("\n1. Check if PostgreSQL is running:")
        print("   docker ps | grep postgres")
        print("\n2. Check PostgreSQL logs:")
        print("   docker logs token-metrics-postgres")
        print("\n3. Verify password in .env matches docker-compose.full.yml:")
        print("   POSTGRES_PASSWORD=postgres_secure_password")
        print("\n4. If PostgreSQL in Docker, check .env settings:")
        print("   POSTGRES_HOST=localhost  (for local access)")
        print("   POSTGRES_HOST=postgres   (for docker-to-docker)")
        print("\n5. Reset PostgreSQL container:")
        print("   docker-compose -f docker/docker-compose.full.yml down -v")
        print("   docker-compose -f docker/docker-compose.full.yml up postgres -d")
        print("\n6. Wait for PostgreSQL to be ready:")
        print("   docker exec token-metrics-postgres pg_isready -U postgres")
        print()
        return 1


if __name__ == '__main__':
    sys.exit(test_connection())

FROM postgres:16-alpine

# Set environment variables
ENV POSTGRES_DB=token_metrics
ENV POSTGRES_USER=postgres
ENV POSTGRES_PASSWORD=postgres

# Copy initialization script
COPY init.sql /docker-entrypoint-initdb.d/

# Create data directory
RUN mkdir -p /var/lib/postgresql/data && \
    chown -R postgres:postgres /var/lib/postgresql

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD pg_isready -U postgres -d token_metrics || exit 1

#!/bin/bash
set -e

# Wait for database to be ready
if [ "${DATABASE_URL:-}" ]; then
    echo "Waiting for database..."
    
    host=$(echo $DATABASE_URL | awk -F[@/] '{print $4}' | awk -F: '{print $1}')
    port=$(echo $DATABASE_URL | awk -F[@/] '{print $4}' | awk -F: '{print $2}')
    
    if [ -z "$port" ]; then
        port=5432
    fi
    
    until PGPASSWORD=$POSTGRES_PASSWORD pg_isready -h $host -p $port -U $POSTGRES_USER; do
        echo "Database not ready, waiting..."
        sleep 2
    done
    
    echo "Database is ready!"
fi

# Apply database migrations
echo "Applying database migrations..."
alembic upgrade head

# Execute the provided command
exec "$@"
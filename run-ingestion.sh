#!/bin/bash
set -e

echo "=============================================="
echo "DataSync Ingestion - Running Solution"
echo "=============================================="

# Start the ingestion services
echo "Starting services..."
docker compose up -d --build

echo ""
echo "Waiting for services to initialize..."
sleep 10

# Monitor progress
echo ""
echo "Monitoring ingestion progress..."
echo "(Press Ctrl+C to stop monitoring)"
echo "=============================================="

while true; do
    COUNT=$(docker exec assignment-postgres psql -U postgres -d ingestion -t -c "SELECT GREATEST(reltuples::bigint, 0) FROM pg_class WHERE relname = 'ingested_events';" 2>/dev/null | tr -d ' ' || echo "0")

    if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
        echo ""
        echo "=============================================="
        echo "INGESTION COMPLETE!"
        echo "Total events: $COUNT"
        echo "=============================================="
        exit 0
    fi

    echo "[$(date '+%H:%M:%S')] Events ingested: $COUNT"
    sleep 5
done

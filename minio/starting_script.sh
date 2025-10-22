#!/bin/bash
set -e

# Running MinIO in background
minio server /data --console-address ":9001" &

# Waiting for MinIO to start
echo "Waiting for MinIO to start..."
until curl -s http://minio:9100/minio/health/live > /dev/null; do
  sleep 1
done

# Starting init script
echo "Running initialization script..."
/docker-entrypoint-initdb.d/init.sh

# Retranslating MinIO from background to foreground
wait
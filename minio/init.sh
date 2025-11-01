#!/bin/bash
set -e

echo "Waiting for MinIO to start..."

# Delay for full initialization of MinIO
sleep 5

# Using env for creating aliases
USERNAME=$(cat ${MINIO_ROOT_USER_FILE})
PASSWORD=$(cat ${MINIO_ROOT_PASSWORD_FILE})

# Configure mc alias
set +o history
/usr/bin/mc alias set myminio http://minio:9000 "${USERNAME}" "${PASSWORD}"
set -o history

# Create bucket
/usr/bin/mc mb myminio/forecast-data --ignore-existing

# Check bucket creation
/usr/bin/mc ls myminio/forecast-data

# Setting bucket to open for downloading
# That will keep some services independent from S3 as a whole
/usr/bin/mc anonymous set download myminio/forecast-data

echo "MinIO initialization completed successfully"

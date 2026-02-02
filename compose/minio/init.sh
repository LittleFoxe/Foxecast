#!/bin/bash
echo "Waiting for MinIO to start..."

# Delay for full initialization of MinIO
sleep 5

# Using env for creating aliases
USERNAME=$MINIO_ROOT_USER
PASSWORD=$MINIO_ROOT_PASSWORD

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

# Downloading sample file for testing
if [ -f /data/sample.grib2 ]; then
    echo "Uploading sample.grib2 to the bucket..."
    /usr/bin/mc cp /data/sample.grib2 myminio/forecast-data/
    echo "File sample.grib2 uploaded successfully!"
else
    echo "Warning: File /data/sample.grib2 not found! Bucket is empty."
fi

echo "MinIO initialization completed successfully"

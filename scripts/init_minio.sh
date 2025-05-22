#!/bin/sh
set -e

# Wait for MinIO to be ready
until mc alias set minio http://minio:9000 minioadmin minioadmin; do
  echo "Waiting for MinIO..."
  sleep 1
done


echo "MinIO initialized"
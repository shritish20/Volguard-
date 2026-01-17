#!/bin/bash
set -e

# Ensure the database directory exists
mkdir -p /app/data
mkdir -p /app/logs

# Start the application
# We explicitly force port 8080 to match Docker/Prometheus config
# We force unbuffered output so logs appear in Docker immediately
exec python -u volguard.py --mode auto --metrics-port 8080 --skip-confirm


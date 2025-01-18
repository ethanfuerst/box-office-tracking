#!/bin/bash

set -e

echo "Starting deployment of sync_and_update.py..."

if poetry run modal deploy sync_and_update.py; then
    echo "Deployment completed successfully."
else
    echo "Deployment failed." >&2
    exit 1
fi

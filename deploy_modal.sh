#!/bin/bash

set -e

FILE="sync_and_update.py"

log() {
    local message="$1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $message"
}

if ! command -v poetry &> /dev/null; then
    log "Poetry is not installed. Please install it first." >&2
    exit 1
fi

if ! command -v modal &> /dev/null; then
    log "Modal is not installed. Please install it first." >&2
    exit 1
fi

log "Starting deployment of $FILE..."

poetry run modal deploy "$FILE"

if [ $? -eq 0 ]; then
    log "Deployment completed successfully."
else
    log "Deployment failed." >&2
    exit 1
fi

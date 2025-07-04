#!/bin/bash

set -e

FILE="sync_and_update.py"

log() {
    local message="$1"
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $message"
}

if ! command -v uv &> /dev/null; then
    log "uv is not installed. Please install it first." >&2
    exit 1
fi

if ! command -v modal &> /dev/null; then
    log "Modal is not installed. Please install it first." >&2
    exit 1
fi

log "Starting deployment of $FILE..."
# Install dependencies using uv
uv sync

# Check if modal is available
uv run modal --version

# Deploy using uv
uv run modal deploy "$FILE"

if [ $? -eq 0 ]; then
    log "Deployment completed successfully."
else
    log "Deployment failed." >&2
    exit 1
fi

#!/bin/bash

# Usage: ./run_dbt.sh [subdirectory] [dbt_commands...]
# Example: ./run_dbt.sh staging run
# Example: ./run_dbt.sh analytics test
# Example: ./run_dbt.sh run (uses default: staging)

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Get subdirectory argument or use default
if [ $# -eq 0 ]; then
    echo "Usage: $0 [subdirectory] [dbt_commands...]"
    echo "Example: $0 staging run"
    echo "Example: $0 analytics test"
    exit 1
fi

# Check if first argument is a dbt command (no subdirectory provided)
DBT_COMMANDS="run test compile build docs seed snapshot source freshness debug"
FIRST_ARG="$1"
SUBDIR="staging"  # default subdirectory

# If first argument is a dbt command, use default subdirectory
if echo "$DBT_COMMANDS" | grep -q "\b$FIRST_ARG\b"; then
    DBT_ARGS="$@"
else
    # First argument is subdirectory
    SUBDIR="$1"
    shift
    DBT_ARGS="$@"
fi

# Validate subdirectory exists
if [ ! -d "dbt_subprojects/$SUBDIR" ]; then
    echo "Error: Directory dbt_subprojects/$SUBDIR does not exist"
    echo "Available subdirectories:"
    ls -1 dbt_subprojects/ 2>/dev/null || echo "  No dbt_subprojects directory found"
    exit 1
fi

echo "Running dbt in dbt_subprojects/$SUBDIR with args: $DBT_ARGS"

# Change to dbt project directory and run dbt
cd "dbt_subprojects/$SUBDIR" && uv run dbt $DBT_ARGS
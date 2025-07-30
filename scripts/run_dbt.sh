#!/bin/bash

# Load environment variables from .env file
if [ -f .env ]; then
    export $(cat .env | grep -v '^#' | xargs)
fi

# Change to dbt project directory and run dbt
cd dbt_subprojects/staging && uv run dbt "$@"
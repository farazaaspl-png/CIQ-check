#!/bin/sh

echo "Environment Tag: $ENVIRONMENT_TAG"

if [ "$ENVIRONMENT_TAG" = "dev" ] || [ "$ENVIRONMENT_TAG" = "perf" ] || [ "$ENVIRONMENT_TAG" = "prod" ]; then
    python deployment/get_config_v2.py
else
    echo "Unknown environment: $ENVIRONMENT_TAG"
    exit 1
fi

# Run database script only for perf and prod
if [ "$ENVIRONMENT_TAG" = "perf" ] || [ "$ENVIRONMENT_TAG" = "prod" ]; then
    python database_scripts/main.py
fi
# Continue with the rest of your scripts
exec python main.py

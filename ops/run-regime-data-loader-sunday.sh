#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
PROJECT_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
CONFIG_FILE="$PROJECT_ROOT/config.yaml"
PYTHON="$PROJECT_ROOT/.venv/bin/python"
CLI="$PROJECT_ROOT/.venv/bin/regime-data-loader"
LOG_DIR="$PROJECT_ROOT/.logs"
LOG_PATH="$LOG_DIR/regime-data-loader.log"

mkdir -p "$LOG_DIR"
exec >>"$LOG_PATH" 2>&1

printf '\n[%s] Starting Sunday regime-data-loader job\n' "$(date --iso-8601=seconds)"

eval "$("$PYTHON" "$PROJECT_ROOT/scripts/export_cron_config.py" "$CONFIG_FILE")"

printf '[%s] Running delta-only daily pipeline\n' "$(date --iso-8601=seconds)"
"$CLI" --lake-root "$LAKE_ROOT" run-daily

printf '[%s] Synchronizing canonical Gold to PostgreSQL\n' "$(date --iso-8601=seconds)"
"$CLI" --lake-root "$LAKE_ROOT" gold-sync-postgres

printf '[%s] Sunday regime-data-loader job completed\n' "$(date --iso-8601=seconds)"

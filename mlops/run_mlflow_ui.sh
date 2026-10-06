#!/usr/bin/env bash
# ==============================================================================
# MLflow Tracking & Model Registry Web UI Runner
# ==============================================================================
# Launches MLflow UI to view experiments, runs, metrics, parameters,
# artifacts, and the Model Registry.
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PORT="${1:-5000}"

export MLFLOW_DISABLE_AGENT_HINT=1

echo "======================================================================"
echo " Launching MLflow Tracking Server UI"
echo "======================================================================"
echo "Tracking DB: sqlite:///$PROJECT_ROOT/mlflow.db"
echo "Host:        http://0.0.0.0:$PORT (Local: http://localhost:$PORT)"
echo "======================================================================"

cd "$PROJECT_ROOT"
mlflow ui \
  --backend-store-uri "sqlite:///$PROJECT_ROOT/mlflow.db" \
  --port "$PORT" \
  --host 0.0.0.0

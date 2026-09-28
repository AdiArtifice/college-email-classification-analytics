#!/usr/bin/env bash
# ==============================================================================
# Intelligent College Email Classification — Streamlit Dashboard Runner (Phase 7)
# ==============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

PORT="${1:-8501}"

echo "======================================================================"
echo " Launching Streamlit Dashboard"
echo "======================================================================"
echo "Project Root: $PROJECT_ROOT"
echo "Port:         $PORT"
echo "URL:          http://localhost:$PORT"
echo "======================================================================"

cd "$PROJECT_ROOT"

python3 -m streamlit run "$SCRIPT_DIR/app.py" \
  --server.port="$PORT" \
  --server.headless=true \
  --server.address=0.0.0.0

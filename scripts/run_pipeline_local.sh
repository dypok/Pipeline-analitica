#!/bin/bash
set -e

# Establecer PYTHONPATH al directorio raíz
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
export PYTHONPATH="$BASE_DIR:$PYTHONPATH"

echo "Ejecutando pipeline Medallion localmente..."
python3 "$BASE_DIR/scripts/run_pipeline_local.py"

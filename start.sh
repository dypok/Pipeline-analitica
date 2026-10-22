#!/usr/bin/env bash
# ==============================================================================
# MEDALLION ANALYTICS PLATFORM - Script de Inicio Automático para Linux / macOS
# Proyecto: Pipeline-analitica
# ==============================================================================

set -e
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT_DIR"

show_menu() {
    clear
    echo "==============================================================================="
    echo "            PLATAFORMA ANALITICA MEDALLION - PANEL DE CONTROL (LINUX)"
    echo "==============================================================================="
    echo ""
    echo "  [1] Iniciar Solo Aplicación Web (FastAPI :8000 + Streamlit :8501 en modo local)"
    echo "  [2] Ejecutar Suite Completa de Pruebas (Pytest)"
    echo "  [3] Iniciar Stack Docker Completo (MinIO, Spark, Airflow, Postgres)"
    echo "  [4] Ejecutar Pipeline Local End-to-End (scripts/run_pipeline_local.py)"
    echo "  [5] Generar / Regenerar Datos Sintéticos (scripts/generate_mock_data.py)"
    echo "  [6] Ejecutar Módulo de Gobernanza y Linaje (Atlas & OpenMetadata)"
    echo "  [7] Detener Contenedores Docker y Servidores Locales"
    echo "  [8] Salir"
    echo ""
    echo "==============================================================================="
    read -p "Seleccione una opción [1-8] (por defecto 1): " OPT
    OPT="${OPT:-1}"

    case "$OPT" in
        1) start_app ;;
        2) run_tests ;;
        3) start_docker ;;
        4) run_pipeline ;;
        5) gen_mock ;;
        6) run_governance ;;
        7) stop_all ;;
        8) exit 0 ;;
        *) echo "[ERROR] Opción no válida."; exit 1 ;;
    esac
}

ensure_env() {
    if [ ! -f ".env" ]; then
        echo "[INFO] Archivo .env no encontrado. Creando a partir de .env.example..."
        cp .env.example .env
        echo "[OK] Archivo .env creado."
    fi
}

start_app() {
    ensure_env
    echo "[*] Iniciando Backend FastAPI en segundo plano (puerto 8000)..."
    .venv/bin/uvicorn app.backend.main:app --host 0.0.0.0 --port 8000 --reload &
    BACKEND_PID=$!

    echo "[*] Iniciando Dashboard Streamlit (puerto 8501)..."
    .venv/bin/streamlit run dashboards/main.py --server.port 8501 --server.headless true &
    STREAMLIT_PID=$!

    echo ""
    echo "==============================================================================="
    echo "  [OK] Servicios Iniciados:"
    echo "       - Dashboard Streamlit: http://localhost:8501"
    echo "       - API Backend FastAPI: http://localhost:8000"
    echo "       - Documentación API:   http://localhost:8000/docs"
    echo "==============================================================================="
    echo "Presione Ctrl+C para detener ambos servidores."
    trap "kill $BACKEND_PID $STREAMLIT_PID 2>/dev/null || true; exit 0" INT TERM
    wait
}

run_tests() {
    echo "[*] Ejecutando suite de pruebas automatizadas con Pytest..."
    .venv/bin/pytest tests/ -v
}

start_docker() {
    ensure_env
    echo "[*] Levantando infraestructura con Docker Compose..."
    docker compose up -d
    echo "[OK] Contenedores iniciados. MinIO en http://localhost:9001, Airflow en http://localhost:8080."
}

run_pipeline() {
    ensure_env
    echo "[*] Ejecutando pipeline Medallion localmente..."
    .venv/bin/python scripts/run_pipeline_local.py
}

gen_mock() {
    echo "[*] Generando datasets sintéticos..."
    .venv/bin/python scripts/generate_mock_data.py --records 5000
}

run_governance() {
    echo "[*] Ejecutando catálogo de Apache Atlas..."
    .venv/bin/python governance/atlas/classify_entities.py
    echo "[*] Ejecutando emisor de linaje OpenMetadata..."
    .venv/bin/python governance/openmetadata/lineage_emitter.py
}

stop_all() {
    echo "[*] Deteniendo contenedores Docker..."
    docker compose down 2>/dev/null || true
    echo "[*] Deteniendo procesos uvicorn y streamlit..."
    pkill -f "uvicorn app.backend.main:app" 2>/dev/null || true
    pkill -f "streamlit run dashboards/main.py" 2>/dev/null || true
    echo "[OK] Todos los servicios detenidos."
}

# Manejo de argumentos CLI
ACTION="${1:-menu}"
case "$ACTION" in
    menu) show_menu ;;
    app) start_app ;;
    test) run_tests ;;
    docker) start_docker ;;
    pipeline) run_pipeline ;;
    mock) gen_mock ;;
    governance) run_governance ;;
    stop) stop_all ;;
    *)
        echo "Uso: ./start.sh [menu|app|test|docker|pipeline|mock|governance|stop]"
        exit 1
        ;;
esac

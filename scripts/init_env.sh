#!/bin/bash
set -e

echo "=========================================================="
echo " MEDALLION PLATFORM — Inicialización del Entorno Local"
echo "=========================================================="

# 1. Comprobar Docker y Docker Compose
if ! command -v docker &> /dev/null; then
    echo "[ERROR] Docker no está instalado o no se encuentra en el PATH."
    exit 1
fi

if ! docker compose version &> /dev/null; then
    echo "[ERROR] Docker Compose v2 no está disponible."
    exit 1
fi

echo "[OK] Docker y Docker Compose detectados correctamente."

# 2. Configurar .env si no existe
if [ ! -f .env ]; then
    echo "[INFO] Creando archivo .env a partir de .env.example..."
    cp .env.example .env
else
    echo "[INFO] Archivo .env ya existe."
fi

# 3. Crear directorios locales requeridos
mkdir -p data/mock logs

echo "[OK] Estructura de directorios lista."
echo "Para levantar la infraestructura ejecuta:"
echo "  docker compose up -d"

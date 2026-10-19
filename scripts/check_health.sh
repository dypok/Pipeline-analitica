#!/bin/bash

echo "=========================================================="
echo " MEDALLION PLATFORM — DIAGNÓSTICO DE SALUD Y SERVICIOS"
echo "=========================================================="

echo -e "\n--- 1. Estado de Contenedores Docker ---"
docker compose ps

echo -e "\n--- 2. Verificación de Puertos y Endpoints ---"

check_endpoint() {
    local name="$1"
    local url="$2"
    if curl -s -f -o /dev/null "$url"; then
        echo -e "[OK] $name está respondiendo ($url)"
    else
        echo -e "[WARN] $name NO responde aún ($url)"
    fi
}

check_endpoint "MinIO API" "http://localhost:9000/minio/health/live"
check_endpoint "MinIO Console UI" "http://localhost:9001"
check_endpoint "Spark Master WebUI" "http://localhost:8081"
check_endpoint "Airflow Webserver" "http://localhost:8080/health"

echo -e "\n--- 3. Verificación de PostgreSQL Analítico (Puerto 5433) ---"
if nc -z localhost 5433 2>/dev/null || timeout 2 bash -c "</dev/tcp/localhost/5433" 2>/dev/null; then
    echo "[OK] Puerto PostgreSQL 5433 abierto y escuchando conexiones."
else
    echo "[WARN] Puerto PostgreSQL 5433 no responde todavía."
fi

echo -e "\n=========================================================="

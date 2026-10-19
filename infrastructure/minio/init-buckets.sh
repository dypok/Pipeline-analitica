#!/bin/sh
set -e

echo "=== [MinIO Init] Esperando a que el servicio MinIO esté listo ==="
until /usr/bin/mc alias set myminio "$MINIO_ENDPOINT" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"; do
    echo "Esperando conexión con MinIO en $MINIO_ENDPOINT..."
    sleep 2
done

echo "=== [MinIO Init] Conexión establecida. Creando buckets de la arquitectura Medallion ==="

BUCKETS="landing bronze silver gold quarantine"

for bucket in $BUCKETS; do
    if /usr/bin/mc ls "myminio/$bucket" > /dev/null 2>&1; then
        echo "-> Bucket '$bucket' ya existe."
    else
        echo "-> Creando bucket '$bucket'..."
        /usr/bin/mc mb "myminio/$bucket"
    fi
done

echo "=== [MinIO Init] Listado de buckets configurados: ==="
/usr/bin/mc ls myminio

echo "=== [MinIO Init] Inicialización completada con éxito ==="

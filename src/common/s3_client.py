"""Cliente de almacenamiento de objetos compatible con MinIO / Amazon S3."""

import io
import json
from typing import Any, List, Optional
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from src.common.config import get_settings
from src.common.logging_utils import get_logger

logger = get_logger("S3Client")


class S3Client:
    """Cliente para operaciones de lectura/escritura en MinIO."""

    def __init__(
        self,
        endpoint_url: Optional[str] = None,
        access_key: Optional[str] = None,
        secret_key: Optional[str] = None,
        region_name: Optional[str] = None,
    ):
        settings = get_settings()
        self.endpoint_url = endpoint_url or settings.minio_endpoint
        self.access_key = access_key or settings.minio_access_key
        self.secret_key = secret_key or settings.minio_secret_key
        self.region_name = region_name or settings.minio_region

        self._s3 = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def ensure_bucket_exists(self, bucket_name: str) -> bool:
        """Crea el bucket si no existe."""
        try:
            self._s3.head_bucket(Bucket=bucket_name)
            return True
        except ClientError:
            try:
                self._s3.create_bucket(Bucket=bucket_name)
                logger.info(f"Bucket creado: '{bucket_name}'")
                return True
            except Exception as e:
                logger.error(f"Error al crear bucket '{bucket_name}': {e}")
                raise

    def upload_bytes(self, data: bytes, bucket_name: str, object_name: str, content_type: str = "application/octet-stream") -> str:
        """Sube bytes en memoria a un bucket."""
        self.ensure_bucket_exists(bucket_name)
        try:
            self._s3.put_object(
                Bucket=bucket_name,
                Key=object_name,
                Body=data,
                ContentType=content_type,
            )
            uri = f"s3://{bucket_name}/{object_name}"
            logger.info(f"Archivo subido a MinIO: {uri} ({len(data)} bytes)")
            return uri
        except Exception as e:
            logger.error(f"Error subiendo bytes a '{bucket_name}/{object_name}': {e}")
            raise

    def upload_json(self, data: Any, bucket_name: str, object_name: str) -> str:
        """Serializa un objeto Python a JSON y lo sube."""
        payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        return self.upload_bytes(payload, bucket_name, object_name, content_type="application/json")

    def upload_file(self, file_path: str, bucket_name: str, object_name: str) -> str:
        """Sube un archivo del sistema de archivos local a MinIO."""
        self.ensure_bucket_exists(bucket_name)
        try:
            self._s3.upload_file(file_path, bucket_name, object_name)
            uri = f"s3://{bucket_name}/{object_name}"
            logger.info(f"Archivo local '{file_path}' subido a: {uri}")
            return uri
        except Exception as e:
            logger.error(f"Error subiendo archivo '{file_path}' a '{bucket_name}/{object_name}': {e}")
            raise

    def read_bytes(self, bucket_name: str, object_name: str) -> bytes:
        """Lee el contenido binario de un objeto."""
        try:
            response = self._s3.get_object(Bucket=bucket_name, Key=object_name)
            return response["Body"].read()
        except Exception as e:
            logger.error(f"Error leyendo objeto '{bucket_name}/{object_name}': {e}")
            raise

    def read_json(self, bucket_name: str, object_name: str) -> Any:
        """Lee y deserializa un objeto JSON desde MinIO."""
        raw_bytes = self.read_bytes(bucket_name, object_name)
        return json.loads(raw_bytes.decode("utf-8"))

    def list_objects(self, bucket_name: str, prefix: str = "") -> List[str]:
        """Lista las claves de objetos dentro de un bucket y prefijo."""
        self.ensure_bucket_exists(bucket_name)
        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            keys: List[str] = []
            for page in pages:
                for obj in page.get("Contents", []):
                    keys.append(obj["Key"])
            return keys
        except Exception as e:
            logger.error(f"Error listando objetos en '{bucket_name}' con prefijo '{prefix}': {e}")
            raise

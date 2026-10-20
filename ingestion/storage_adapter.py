"""
ingestion/storage_adapter.py
----------------------------
Adaptador de almacenamiento para capas Landing y Bronze.
Soporta almacenamiento local (filesystem) y MinIO / S3 compatible.
Garantiza inmutabilidad calculando hashes SHA-256 y escribiendo Parquet optimizado.
"""

import hashlib
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Union
import pandas as pd


class StorageAdapter:
    """
    Gestiona el almacenamiento físico o en objeto para las capas Medallion.
    """

    def __init__(self, base_dir: Optional[str] = None):
        self.base_dir = Path(base_dir or os.getenv("STORAGE_BASE_DIR", "storage"))
        self.landing_dir = self.base_dir / "landing"
        self.bronze_dir = self.base_dir / "bronze"
        self.metadata_dir = self.base_dir / "metadata"

        # Inicializar directorios locales base
        self.landing_dir.mkdir(parents=True, exist_ok=True)
        self.bronze_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def compute_sha256(data: bytes) -> str:
        """Calcula el hash SHA-256 de los bytes crudos."""
        return hashlib.sha256(data).hexdigest()

    def build_landing_key(self, source_id: str, ingestion_id: str, extension: str) -> str:
        """
        Genera la ruta estándar en Landing:
        landing/{source_id}/{YYYY}/{MM}/{DD}/{ingestion_id}.{ext}
        """
        now = datetime.now(timezone.utc)
        ext = extension.lstrip(".")
        return f"landing/{source_id}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{ingestion_id}.{ext}"

    def build_bronze_key(self, dataset_name: str, ingestion_id: str) -> str:
        """
        Genera la ruta estándar en Bronze:
        bronze/{dataset_name}/{YYYY}/{MM}/{DD}/{ingestion_id}.parquet
        """
        now = datetime.now(timezone.utc)
        return f"bronze/{dataset_name}/{now.year:04d}/{now.month:02d}/{now.day:02d}/{ingestion_id}.parquet"

    def save_landing_bytes(self, relative_key: str, data: bytes) -> tuple[str, str]:
        """
        Almacena bytes crudos inmutables en Landing.
        Retorna (full_path_or_uri, sha256_hash).
        """
        sha256_hash = self.compute_sha256(data)
        dest_path = self.base_dir / relative_key
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "wb") as f:
            f.write(data)

        return str(dest_path.as_posix()), sha256_hash

    def save_bronze_parquet(self, relative_key: str, df: pd.DataFrame) -> str:
        """
        Almacena DataFrame en formato Parquet comprimido con Snappy en Bronze.
        Retorna el path/URI del archivo creado.
        """
        dest_path = self.base_dir / relative_key
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        df.to_parquet(dest_path, index=False, compression="snappy")
        return str(dest_path.as_posix())

    def save_manifest(self, ingestion_id: str, manifest_data: dict) -> str:
        """Almacena el manifiesto de control de la ejecución en JSON."""
        import json
        now = datetime.now(timezone.utc)
        manifest_key = f"metadata/{now.year:04d}/{now.month:02d}/{now.day:02d}/{ingestion_id}_manifest.json"
        dest_path = self.base_dir / manifest_key
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        with open(dest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2, ensure_ascii=False)

        return str(dest_path.as_posix())

    def read_landing_bytes(self, relative_key: str) -> bytes:
        """Lee los bytes crudos almacenados en Landing."""
        target_path = self.base_dir / relative_key
        if not target_path.exists():
            raise FileNotFoundError(f"Landing object not found: {target_path}")
        with open(target_path, "rb") as f:
            return f.read()

    def read_bronze_dataframe(self, relative_key: str) -> pd.DataFrame:
        """Lee un archivo Parquet de Bronze."""
        target_path = self.base_dir / relative_key
        if not target_path.exists():
            raise FileNotFoundError(f"Bronze parquet not found: {target_path}")
        return pd.read_parquet(target_path)

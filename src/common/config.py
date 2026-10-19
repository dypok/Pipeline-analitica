"""Módulo centralizado de carga y resolución de configuración."""

import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml
from dotenv import load_dotenv

# Cargar variables de entorno desde .env si existe
load_dotenv()


class Settings:
    """Contenedor de configuración para toda la plataforma Medallion."""

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        self._raw = config_dict or {}

    @classmethod
    def load(cls, config_path: Optional[str] = None) -> "Settings":
        """Carga la configuración desde un archivo YAML y resuelve variables de entorno."""
        if config_path is None:
            base_dir = Path(__file__).resolve().parent.parent.parent
            config_path = str(base_dir / "config" / "settings.yaml")

        data: Dict[str, Any] = {}
        path_obj = Path(config_path)
        if path_obj.exists():
            with open(path_obj, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

        return cls(data)

    @property
    def environment(self) -> str:
        return os.getenv("ENVIRONMENT", self._raw.get("project", {}).get("environment", "development"))

    @property
    def minio_endpoint(self) -> str:
        return os.getenv("MINIO_ENDPOINT") or self._raw.get("storage", {}).get("s3_endpoint", "http://localhost:9000")

    @property
    def minio_host_endpoint(self) -> str:
        return os.getenv("MINIO_HOST_ENDPOINT") or self._raw.get("storage", {}).get("host_endpoint", "http://localhost:9000")

    @property
    def minio_access_key(self) -> str:
        return os.getenv("MINIO_ROOT_USER", "minioadmin")

    @property
    def minio_secret_key(self) -> str:
        return os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123")

    @property
    def minio_region(self) -> str:
        return os.getenv("MINIO_REGION", self._raw.get("storage", {}).get("region", "us-east-1"))

    def get_bucket(self, layer: str) -> str:
        """Retorna el nombre del bucket para una capa Medallion dada."""
        buckets = self._raw.get("storage", {}).get("buckets", {})
        env_var = f"MINIO_BUCKET_{layer.upper()}"
        return os.getenv(env_var, buckets.get(layer.lower(), layer.lower()))

    @property
    def analytics_db_url(self) -> str:
        return os.getenv(
            "ANALYTICS_DATABASE_URL",
            os.getenv("HOST_ANALYTICS_DB_URL", "postgresql+psycopg2://medallion_user:medallion_secret@localhost:5433/analytics_db"),
        )

    @property
    def host_analytics_db_url(self) -> str:
        return os.getenv(
            "HOST_ANALYTICS_DB_URL",
            "postgresql+psycopg2://medallion_user:medallion_secret@localhost:5433/analytics_db",
        )

    @property
    def quality_rules(self) -> Dict[str, Any]:
        return self._raw.get("quality", {}).get("rules", {})

    @property
    def spark_config(self) -> Dict[str, Any]:
        return self._raw.get("spark", {})


_cached_settings: Optional[Settings] = None


def get_settings(config_path: Optional[str] = None) -> Settings:
    """Retorna una instancia singleton o cargada de la configuración."""
    global _cached_settings
    if _cached_settings is None or config_path is not None:
        _cached_settings = Settings.load(config_path)
    return _cached_settings

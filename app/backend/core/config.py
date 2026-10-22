from typing import Optional
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from pathlib import Path


class Settings(BaseSettings):
    """Configuración general del backend y servicios analíticos."""
    ENVIRONMENT: str = "development"
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    PROJECT_NAME: str = "Medallion Data Platform API"
    VERSION: str = "2.0.0"

    # Rutas de datos
    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent.parent
    GOLD_DATA_PATH: str = "data/mock/gold"

    # Configuración de consultas
    MAX_QUERY_ROWS: int = 500
    QUERY_TIMEOUT_SECONDS: int = 5

    # Proveedor LLM para el Chatbot (por defecto modelo local Qwen2.5 sin tiempo límite fijo)
    LLM_PROVIDER: str = "local"  # "local", "gemini", "openai"
    LOCAL_LLM_URL: str = "http://localhost:11434/v1"
    LOCAL_LLM_MODEL: str = "qwen2.5:1.5b"
    LOCAL_LLM_TIMEOUT: Optional[float] = None  # None = sin tiempo límite fijo para inferencia en CPU

    @field_validator("LOCAL_LLM_TIMEOUT", mode="before")
    @classmethod
    def parse_optional_timeout(cls, v):
        if v is None or v == "" or str(v).lower() in ("none", "0", "null", "false"):
            return None
        return float(v)

    GEMINI_API_KEY: str | None = None
    OPENAI_API_KEY: str | None = None
    LLM_MODEL: str = "qwen2.5:1.5b"

    # MinIO (opcional para integración directa en producción)
    MINIO_ENDPOINT: str = "localhost:9000"
    MINIO_ACCESS_KEY: str = "minioadmin"
    MINIO_SECRET_KEY: str = "minioadmin123"
    MINIO_SECURE: bool = False
    MINIO_BUCKET_GOLD: str = "gold"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


settings = Settings()

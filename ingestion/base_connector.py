"""
ingestion/base_connector.py
---------------------------
Contrato abstracto base para los conectores de ingesta (US-04 y US-05).
Responsable: Jefri Calderin.

Define el flujo estandarizado del ciclo de vida de ingesta hacia la Arquitectura Medallion:
1. configure(): Parámetros de fuente y destino.
2. validate(): Comprobación de fuente, conexión y contratos previos.
3. fetch(): Extracción de los datos crudos.
4. save_landing(): Preservación inmutable del payload original con hash SHA-256.
5. save_bronze(): Conversión a Parquet columnar agregando columnas técnicas de auditoría.
6. emit_metadata(): Registro del manifiesto de control operativo.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
import logging
import os
import time
from typing import Any, Optional
import uuid
import pandas as pd

from ingestion.storage_adapter import StorageAdapter

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("ingestion.connector")


@dataclass
class IngestionConfig:
    """Configuración estándar de una ejecución de ingesta."""
    source_id: str
    source_type: str  # CSV, JSON, REST_API, etc.
    dataset_name: str
    schema_version: str = "1.0.0"
    storage_base_dir: Optional[str] = None
    extra_params: dict[str, Any] = field(default_factory=dict)


@dataclass
class IngestionResult:
    """Metadatos de control y resultado final de la ingesta (Cuadro 4.2)."""
    ingestion_id: str
    source_id: str
    source_type: str
    dataset_name: str
    ingested_at: str
    schema_version: str
    status: str  # "SUCCESS", "FAILED"
    record_count: int = 0
    landing_path: Optional[str] = None
    bronze_path: Optional[str] = None
    sha256_hash: Optional[str] = None
    error_message: Optional[str] = None
    execution_time_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


class BaseConnector(ABC):
    """
    Clase abstracta base para todos los conectores de datos.
    Garantiza que cualquier fuente implemente el contrato común de la plataforma.
    """

    def __init__(self, config: Optional[IngestionConfig] = None, storage_adapter: Optional[StorageAdapter] = None):
        self.config = config
        self.storage = storage_adapter or StorageAdapter(
            base_dir=config.storage_base_dir if config else None
        )
        self.ingestion_id = str(uuid.uuid4())
        self.ingested_at = datetime.now(timezone.utc).isoformat()
        self.logger = logging.getLogger(f"connector.{self.__class__.__name__}")

    @abstractmethod
    def configure(self, config: dict[str, Any]) -> None:
        """Configura los parámetros específicos de la fuente y ejecución."""
        pass

    @abstractmethod
    def validate(self) -> bool:
        """
        Valida la disponibilidad de la fuente, esquemas previos o conectividad.
        Debe retornar True si la fuente es válida, o lanzar una excepción / False.
        """
        pass

    @abstractmethod
    def fetch(self) -> Any:
        """
        Extrae los datos desde el origen.
        Retorna la representación de datos crudos (bytes, dict o stream).
        """
        pass

    @abstractmethod
    def save_landing(self, raw_data: Any, source_filename: str) -> tuple[str, str]:
        """
        Guarda la información inalterada en Landing con su hash SHA-256.
        Retorna (landing_path, sha256_hash).
        """
        pass

    @abstractmethod
    def save_bronze(self, data: Any, source_filename: str) -> tuple[str, int]:
        """
        Convierte los datos a Parquet agregando columnas técnicas de auditoría:
        - _ingestion_id
        - _ingested_at
        - _source_file
        Retorna (bronze_path, record_count).
        """
        pass

    def add_audit_columns(self, df: pd.DataFrame, source_file: str) -> pd.DataFrame:
        """Agrega columnas técnicas de auditoría obligatorias para la capa Bronze."""
        df_copy = df.copy()
        df_copy["_ingestion_id"] = self.ingestion_id
        df_copy["_ingested_at"] = self.ingested_at
        df_copy["_source_file"] = os.path.basename(source_file)
        return df_copy

    def emit_metadata(self, result: IngestionResult) -> dict[str, Any]:
        """
        Registra y persiste el manifiesto de control operativo.
        """
        manifest_dict = result.to_dict()
        manifest_path = self.storage.save_manifest(self.ingestion_id, manifest_dict)
        manifest_dict["manifest_path"] = manifest_path
        self.logger.info(f"Manifest emitido correctamente en: {manifest_path}")
        return manifest_dict

    def run(self, source_filename: Optional[str] = None) -> IngestionResult:
        """
        Método plantilla que orquesta el ciclo de vida completo de la ingesta con resiliencia.
        """
        start_time = time.perf_counter()
        self.logger.info(
            f"Iniciando ingesta {self.ingestion_id} para {self.config.dataset_name if self.config else 'unknown'}"
        )

        source_name = source_filename or f"{self.config.dataset_name if self.config else 'dataset'}.raw"

        result = IngestionResult(
            ingestion_id=self.ingestion_id,
            source_id=self.config.source_id if self.config else "generic_source",
            source_type=self.config.source_type if self.config else "UNKNOWN",
            dataset_name=self.config.dataset_name if self.config else "unnamed_dataset",
            ingested_at=self.ingested_at,
            schema_version=self.config.schema_version if self.config else "1.0.0",
            status="FAILED",
            record_count=0
        )

        try:
            # 1. Validación previa
            self.logger.info("Paso 1: Validando fuente...")
            is_valid = self.validate()
            if not is_valid:
                raise ValueError(f"Fallo en validación de la fuente para {result.dataset_name}")

            # 2. Extracción de datos
            self.logger.info("Paso 2: Obteniendo datos (fetch)...")
            raw_data = self.fetch()

            # 3. Guardado en Landing
            self.logger.info("Paso 3: Preservando datos originales en Landing...")
            landing_path, sha256_hash = self.save_landing(raw_data, source_name)
            result.landing_path = landing_path
            result.sha256_hash = sha256_hash

            # 4. Procesamiento hacia Bronze
            self.logger.info("Paso 4: Escribiendo datos estructurados en Bronze (Parquet)...")
            bronze_path, record_count = self.save_bronze(raw_data, source_name)
            result.bronze_path = bronze_path
            result.record_count = record_count

            # Finalización exitosa
            result.status = "SUCCESS"
            self.logger.info(
                f"Ingesta exitosa! {record_count} registros procesados en Landing y Bronze."
            )

        except Exception as e:
            self.logger.error(f"Error crítico durante la ingesta: {str(e)}", exc_info=True)
            result.status = "FAILED"
            result.error_message = str(e)
            # En caso de fallo, no se crea un Bronze roto
            result.bronze_path = None

        finally:
            result.execution_time_seconds = round(time.perf_counter() - start_time, 4)
            self.emit_metadata(result)

        return result

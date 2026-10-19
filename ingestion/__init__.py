"""
ingestion
---------
Capa de ingesta para la plataforma de datos (Arquitectura Medallion).
Responsable del módulo: Jefri Calderin.

Exporta:
- BaseConnector: Contrato abstracto común de ingesta.
- IngestionConfig: Configuración estándar de la fuente y ejecución.
- IngestionResult: Manifiesto y resultado de la ingesta (Cuadro 4.2).
- CSVConnector: Conector para archivos CSV delimitados (US-04).
- JSONConnector: Conector para archivos JSON y NDJSON (US-04).
- RestApiConnector: Conector resiliente para APIs REST paginadas (US-05).
- StorageAdapter: Adaptador de almacenamiento para capas Landing y Bronze.
"""

from ingestion.base_connector import BaseConnector, IngestionConfig, IngestionResult
from ingestion.csv_connector import CSVConnector
from ingestion.json_connector import JSONConnector
from ingestion.rest_api_connector import RestApiConnector
from ingestion.storage_adapter import StorageAdapter

__all__ = [
    "BaseConnector",
    "IngestionConfig",
    "IngestionResult",
    "CSVConnector",
    "JSONConnector",
    "RestApiConnector",
    "StorageAdapter"
]

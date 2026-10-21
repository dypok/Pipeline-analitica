"""Módulo de ingesta y conectores hacia la capa Landing (Responsable: Jefri Calderin)."""
from src.ingestion.base_connector import BaseConnector, IngestionResult
from src.ingestion.sample_generator import MockDataGenerator
from src.ingestion.file_ingestor import LandingIngestor
from src.ingestion.csv_connector import CSVConnector
from src.ingestion.json_connector import JSONConnector
from src.ingestion.rest_api_connector import RestApiConnector

__all__ = [
    "BaseConnector",
    "IngestionResult",
    "MockDataGenerator",
    "LandingIngestor",
    "CSVConnector",
    "JSONConnector",
    "RestApiConnector",
]

"""Módulo de ingesta y conectores hacia la capa Landing."""
from src.ingestion.base_connector import BaseConnector, IngestionResult
from src.ingestion.sample_generator import MockDataGenerator
from src.ingestion.file_ingestor import LandingIngestor

__all__ = ["BaseConnector", "IngestionResult", "MockDataGenerator", "LandingIngestor"]

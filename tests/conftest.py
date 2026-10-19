"""Fixtures globales para pruebas unitarias e integración de la plataforma Medallion."""

import pytest
from src.common.config import Settings, get_settings
from src.ingestion.sample_generator import MockDataGenerator
from src.quality.validator import DataQualityValidator


@pytest.fixture(scope="session")
def settings() -> Settings:
    """Fixture que entrega la configuración de prueba."""
    return get_settings()


@pytest.fixture(scope="session")
def mock_generator() -> MockDataGenerator:
    """Generador con semilla fija para reproducibilidad."""
    return MockDataGenerator(seed=123)


@pytest.fixture(scope="session")
def validator() -> DataQualityValidator:
    """Validador de calidad de datos."""
    return DataQualityValidator()

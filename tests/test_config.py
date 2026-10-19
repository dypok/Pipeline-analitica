"""Pruebas de configuración y carga de parámetros."""

from src.common.config import Settings


def test_settings_load_defaults(settings: Settings):
    """Verifica que los buckets y endpoints carguen correctamente."""
    assert settings.get_bucket("landing") == "landing"
    assert settings.get_bucket("bronze") == "bronze"
    assert settings.get_bucket("silver") == "silver"
    assert settings.get_bucket("gold") == "gold"
    assert settings.get_bucket("quarantine") == "quarantine"

    assert "9000" in settings.minio_endpoint
    assert settings.minio_access_key != ""
    assert settings.minio_secret_key != ""


def test_quality_rules_present(settings: Settings):
    """Verifica que las reglas de calidad estén cargadas en el archivo YAML."""
    rules = settings.quality_rules
    assert "orders" in rules
    assert "required_fields" in rules["orders"]
    assert "order_id" in rules["orders"]["required_fields"]

"""Pruebas de Gobernanza, Catálogo y Linaje de Datos con OpenMetadata / Atlas (US-09).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Cubre:
- Prueba T09.1 (P10 de aceptación): Linaje completo upstream de daily_sales hasta las fuentes CSV/API.
- Prueba T09.2 (P11 de aceptación): Clasificación PII en customers (email, phone).
- Clasificación SENSITIVE_FINANCIAL en modelos Gold y Silver.
- Exportación y consistencia de los manifiestos de gobernanza para OpenMetadata.
"""

from pathlib import Path
from src.governance.classification import ClassificationEngine
from src.governance.data_catalog import DataCatalog
from src.governance.lineage_tracker import LineageTracker
from src.governance.openmetadata_client import OpenMetadataClient


def test_lineage_graph_e2e_p10():
    """Prueba T09.1 (P10 de aceptación): El dataset daily_sales debe rastrearse

    upstream hasta las fuentes originales (CSV / API) a través de Silver, Bronze y Landing.
    """
    tracker = LineageTracker()
    upstream = tracker.get_upstream_lineage("gold.daily_sales")

    # Debe contener la cadena completa de capas Medallion
    assert "silver.orders" in upstream
    assert "bronze.orders" in upstream
    assert "landing.orders" in upstream
    assert "src.csv_orders" in upstream or "src.api_sales" in upstream

    # Verificar downstream desde las fuentes
    downstream = tracker.get_downstream_lineage("landing.orders")
    assert "bronze.orders" in downstream
    assert "silver.orders" in downstream
    assert "gold.daily_sales" in downstream
    assert "consumption.dashboard_sales" in downstream


def test_pii_classification_p11():
    """Prueba T09.2 (P11 de aceptación): Consultar la entidad customers y comprobar

    que los atributos de datos personales posean la etiqueta PII.
    """
    engine = ClassificationEngine()

    assert engine.is_pii("customers", "email") is True
    assert engine.is_pii("customers", "phone") is True
    assert engine.is_pii("customers", "first_name") is True
    assert engine.is_pii("customers", "last_name") is True

    # Atributos no confidenciales no deben ser PII
    assert engine.is_pii("customers", "customer_id") is False
    assert engine.is_pii("customers", "country") is False

    pii_cols = engine.get_entity_pii_columns("customers")
    assert "email" in pii_cols
    assert "phone" in pii_cols


def test_sensitive_financial_classification():
    """Valida la clasificación de atributos financieros y comerciales confidenciales."""
    engine = ClassificationEngine()

    assert engine.is_sensitive_financial("orders", "total_amount") is True
    assert engine.is_sensitive_financial("orders", "unit_price") is True
    assert engine.is_sensitive_financial("daily_sales", "total_revenue") is True
    assert engine.is_sensitive_financial("customer_360", "lifetime_value") is True
    assert engine.is_sensitive_financial("product_performance", "gross_revenue") is True


def test_quarantine_lineage_branch():
    """Verifica que la zona Quarantine esté conectada al flujo de linaje desde el Quality Gate."""
    tracker = LineageTracker()
    upstream_q = tracker.get_upstream_lineage("quarantine.anomalies")

    assert "bronze.orders" in upstream_q
    assert "landing.orders" in upstream_q


def test_data_catalog_schemas_and_descriptions():
    """Valida que el catálogo de datos contenga todas las entidades del Lakehouse con tipos y descripciones."""
    catalog = DataCatalog()
    tables = catalog.list_tables()

    assert "silver.orders" in tables
    assert "silver.customers" in tables
    assert "gold.daily_sales" in tables
    assert "gold.customer_360" in tables
    assert "quarantine.anomalies" in tables

    # Verificar metadatos y clasificación de columnas
    daily_sales = catalog.get_table("gold.daily_sales")
    assert daily_sales is not None
    assert daily_sales.layer == "GOLD"
    revenue_col = next(c for c in daily_sales.columns if c.name == "total_revenue")
    assert revenue_col.classification == "SENSITIVE_FINANCIAL"

    cust_table = catalog.get_table("silver.customers")
    assert cust_table is not None
    email_col = next(c for c in cust_table.columns if c.name == "email")
    assert email_col.classification == "PII"


def test_openmetadata_manifests_export(tmp_path=None):
    """Valida la exportación de manifiestos JSON listos para OpenMetadata y Apache Atlas."""
    if tmp_path is None:
        import tempfile
        tmp_path = Path(tempfile.mkdtemp())
    client = OpenMetadataClient(manifest_dir=tmp_path)
    result = client.export_governance_manifests()

    assert Path(result["catalog"]).is_file()
    assert Path(result["lineage"]).is_file()
    assert Path(result["atlas"]).is_file()

    # Validar sincronización
    sync_res = client.sync_governance()
    assert sync_res["tables_cataloged"] >= 5
    assert sync_res["lineage_nodes"] > 10
    assert sync_res["lineage_edges"] > 10

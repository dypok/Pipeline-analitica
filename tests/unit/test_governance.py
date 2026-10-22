"""Pruebas unitarias para el módulo de Gobernanza y Linaje (US-09).
Valida las clasificaciones de Apache Atlas (PII, SENSITIVE_FINANCIAL) y el grafo de linaje de OpenMetadata.
"""

from governance.atlas.classify_entities import AtlasClassifier
from governance.openmetadata.lineage_emitter import OpenMetadataLineageEmitter


def test_atlas_classification_definitions():
    classifier = AtlasClassifier()
    manifest = classifier.create_classifications()
    assert manifest["status"] in ("SYNCHRONIZED", "OFFLINE_AUDITED")
    assert len(manifest["classifications"]) >= 3

    # Validación P11: email debe ser PII, total_amount debe ser SENSITIVE_FINANCIAL
    assert classifier.get_entity_classification("customers", "email") == "PII"
    assert classifier.get_entity_classification("orders", "total_amount") == "SENSITIVE_FINANCIAL"
    assert classifier.get_entity_classification("orders", "order_id") == "PUBLIC"


def test_openmetadata_lineage_graph():
    emitter = OpenMetadataLineageEmitter()
    manifest = emitter.emit_lineage()
    assert manifest["total_nodes"] == 7
    assert manifest["total_edges"] == 6

    node_ids = [n["id"] for n in manifest["nodes"]]
    assert "landing.orders" in node_ids
    assert "bronze.orders" in node_ids
    assert "silver.orders" in node_ids
    assert "quarantine.orders" in node_ids
    assert "gold.daily_sales" in node_ids
    assert "gold.customer_360" in node_ids
    assert "gold.product_performance" in node_ids

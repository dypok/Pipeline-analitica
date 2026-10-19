"""Pruebas de la capa de ingesta y generación de datos."""

from src.ingestion.sample_generator import MockDataGenerator


def test_mock_customers_generation(mock_generator: MockDataGenerator):
    """Verifica que se generen clientes con las columnas esperadas."""
    customers = mock_generator.generate_customers(count=20)
    assert len(customers) == 20
    first = customers[0]
    assert "customer_id" in first
    assert "email" in first
    assert "country" in first
    assert "registration_date" in first


def test_mock_orders_generation_with_anomalies(mock_generator: MockDataGenerator):
    """Verifica que las órdenes incluyan datos consistentes y anomalías controladas."""
    orders = mock_generator.generate_orders(count=100, customer_count=20)
    assert len(orders) == 100

    anomalous = [o for o in orders if o.get("_test_anomaly") is not None]
    assert len(anomalous) > 0, "Debe haber anomalías deliberadas para probar Quarantine"

    # Verificar presencia de campos
    sample = orders[0]
    expected_fields = ["order_id", "customer_id", "order_date", "total_amount", "status"]
    for f in expected_fields:
        assert f in sample

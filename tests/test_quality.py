"""Pruebas para las reglas de calidad y segregación a Quarantine."""

from src.quality.validator import DataQualityValidator


def test_validate_order_valid_case(validator: DataQualityValidator):
    valid_record = {
        "order_id": "ORD-000001",
        "customer_id": "CUST-00001",
        "order_date": "2026-03-15",
        "total_amount": 125.50,
        "status": "COMPLETED",
    }
    is_valid, reason = validator.validate_order(valid_record)
    assert is_valid is True
    assert reason is None


def test_validate_order_negative_amount(validator: DataQualityValidator):
    invalid_record = {
        "order_id": "ORD-000002",
        "customer_id": "CUST-00001",
        "order_date": "2026-03-15",
        "total_amount": -50.00,
        "status": "COMPLETED",
    }
    is_valid, reason = validator.validate_order(invalid_record)
    assert is_valid is False
    assert "Monto inválido" in reason


def test_validate_order_invalid_status(validator: DataQualityValidator):
    invalid_record = {
        "order_id": "ORD-000003",
        "customer_id": "CUST-00001",
        "order_date": "2026-03-15",
        "total_amount": 75.00,
        "status": "NOT_A_REAL_STATUS",
    }
    is_valid, reason = validator.validate_order(invalid_record)
    assert is_valid is False
    assert "Estado desconocido" in reason


def test_partition_records_quarantine_rate(validator: DataQualityValidator):
    # Lote de 10 registros con 1 solo anómalo (tasa del 10% <= umbral del 15%)
    records = [
        {"order_id": f"O{i}", "customer_id": f"C{i}", "order_date": "2026-01-01", "total_amount": 10.0 * i, "status": "COMPLETED"}
        for i in range(1, 10)
    ]
    # Registro anómalo
    records.append({"order_id": "O10", "customer_id": "C10", "order_date": "2026-01-01", "total_amount": -5.0, "status": "COMPLETED"})

    valid_list, quarantine_list, report = validator.partition_records(records, entity_type="orders")
    assert len(valid_list) == 9
    assert len(quarantine_list) == 1
    assert report.total_records == 10
    assert report.quarantine_records_count == 1
    assert report.quarantine_rate == 0.10
    assert report.passed_threshold is True


def test_partition_records_exceeds_threshold(validator: DataQualityValidator):
    # Lote con alta anomalía (50% de rechazo)
    records = [
        {"order_id": "O1", "customer_id": "C1", "order_date": "2026-01-01", "total_amount": 10.0, "status": "COMPLETED"},
        {"order_id": "O2", "customer_id": "C2", "order_date": "2026-01-01", "total_amount": -10.0, "status": "COMPLETED"},
    ]
    valid_list, quarantine_list, report = validator.partition_records(records, entity_type="orders")
    assert report.quarantine_rate == 0.50
    assert report.passed_threshold is False

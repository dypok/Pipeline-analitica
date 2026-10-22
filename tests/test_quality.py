"""Pruebas completas para las reglas de calidad y segregación a Quarantine (US-08).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Cubre:
- Validación por entidad (orders, customers, products, returns).
- Reglas declarativas desde JSON (retail_rules.json).
- Aislamiento de anomalías a Quarantine con taxonomía de errores técnicos.
- Soporte de Warnings no críticos vs Errores fatales (Prueba T08.2).
- Certificación del lote de 50 registros anómalos conocidos (Prueba T08.1 / P04).
"""

import json
from pathlib import Path
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


# -----------------------------------------------------------------------------
# Nuevas pruebas unitarias específicas de la US-08
# -----------------------------------------------------------------------------

def test_declarative_rules_loaded(validator: DataQualityValidator):
    """Verifica que el validador cargue las reglas desde retail_rules.json."""
    assert "entities" in validator.rules or "orders" in validator.rules
    assert validator.max_quarantine_rate == 0.15


def test_warning_keeps_record_valid_t08_2(validator: DataQualityValidator):
    """Prueba T08.2: Una advertencia no crítica (ej. teléfono no estándar o monto alto)

    NO debe enviar el registro a Quarantine; debe preservarse en Silver con su flag
    de advertencia.
    """
    records = [
        {
            "customer_id": "CUST-0099",
            "email": "user@example.com",
            "country": "CO",
            "phone": "invalid_phone_format_123",  # Advertencia no crítica
        }
    ]
    valid_list, quarantine_list, report = validator.partition_records(records, entity_type="customers")

    assert len(valid_list) == 1
    assert len(quarantine_list) == 0
    assert report.warnings_count == 1
    assert "_quality_warnings" in valid_list[0]
    assert "WARN_NON_STANDARD_PHONE" in valid_list[0]["_quality_warnings"]


def test_quarantine_payload_contract(validator: DataQualityValidator):
    """Verifica que los registros en Quarantine contengan el contrato de metadatos obligatorio."""
    invalid_record = {
        "order_id": "ORD-ERR-TEST",
        "customer_id": "CUST-0001",
        "order_date": "2026-03-01",
        "total_amount": -100.0,
        "status": "COMPLETED",
    }
    _, quarantine_list, _ = validator.partition_records([invalid_record], entity_type="orders")

    assert len(quarantine_list) == 1
    q_rec = quarantine_list[0]
    assert q_rec["_error_code"] == "ERR_NEGATIVE_AMOUNT"
    assert "_error_message" in q_rec
    assert "_rejected_at" in q_rec
    assert q_rec["_original_payload"] == invalid_record
    assert q_rec["entity_type"] == "orders"


def test_batch_50_corrupt_records_t08_1(validator: DataQualityValidator):
    """Prueba T08.1 (P04 de aceptación): Pasar el lote de 50 registros corruptos conocidos

    y certificar que exactamente los 50 van a Quarantine y ninguno contamina Silver.
    """
    file_path = Path(__file__).resolve().parent.parent / "data" / "mock" / "raw" / "corrupt" / "dirty_orders_50.json"
    assert file_path.is_file(), f"No se encontró el dataset {file_path}"

    with open(file_path, "r", encoding="utf-8") as f:
        dirty_records = json.load(f)

    assert len(dirty_records) == 50

    valid_list, quarantine_list, report = validator.partition_records(dirty_records, entity_type="orders")

    # 100% de los registros corruptos deben ser segregados
    assert len(valid_list) == 0
    assert len(quarantine_list) == 50
    assert report.total_records == 50
    assert report.quarantine_records_count == 50
    assert report.quarantine_rate == 1.0
    assert report.passed_threshold is False

    # Verificar que todos tienen código de error y payload original
    for q in quarantine_list:
        assert q["_error_code"].startswith("ERR_")
        assert len(q["_error_message"]) > 0
        assert "_rejected_at" in q
        assert "_original_payload" in q


def test_validate_customer_rules(validator: DataQualityValidator):
    """Prueba validación de email y claves en clientes."""
    # Email inválido
    bad_customer = {"customer_id": "C-1", "email": "not-an-email", "country": "CO"}
    is_valid, reason = validator.validate_customer(bad_customer)
    assert is_valid is False
    assert "Email inválido" in reason

    # Falta clave obligatoria
    missing_id = {"email": "ok@test.com", "country": "CO"}
    is_valid, reason = validator.validate_customer(missing_id)
    assert is_valid is False
    assert "customer_id ausente" in reason


def test_validate_product_rules(validator: DataQualityValidator):
    """Prueba validación de precios y catálogo en productos."""
    bad_product = {"product_id": "P-1", "product_name": "Teclado", "unit_price": -10.0}
    is_valid, reason = validator.validate_product(bad_product)
    assert is_valid is False
    assert "unit_price inválido" in reason


def test_validate_return_rules(validator: DataQualityValidator):
    """Prueba validación de montos de reembolso en devoluciones."""
    bad_return = {"return_id": "R-1", "order_id": "O-1", "refund_amount": -50.0}
    is_valid, reason = validator.validate_return(bad_return)
    assert is_valid is False
    assert "refund_amount negativo" in reason

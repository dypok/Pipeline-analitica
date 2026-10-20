"""Validador de calidad de datos y segregación a Quarantine."""

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any, Dict, List, Optional, Tuple

from src.common.config import get_settings
from src.common.logging_utils import get_logger

logger = get_logger("DataQualityValidator")


@dataclass
class ValidationReport:
    """Reporte detallado de la validación de calidad."""

    total_records: int
    valid_records_count: int
    quarantine_records_count: int
    quarantine_rate: float
    passed_threshold: bool
    reasons_summary: Dict[str, int]


class DataQualityValidator:
    """Aplica reglas de negocio y calidad para segregar datos a Silver o Quarantine."""

    def __init__(self, rules: Optional[Dict[str, Any]] = None):
        settings = get_settings()
        self.rules = rules or settings.quality_rules
        self.email_regex = re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$")

    def validate_order(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Valida una orden de compra según las reglas de negocio."""
        # 1. Campos obligatorios
        required_fields = ["order_id", "customer_id", "order_date", "total_amount", "status"]
        for field in required_fields:
            val = record.get(field)
            if val is None or (isinstance(val, str) and not val.strip()):
                return False, f"Campo obligatorio ausente o vacío: '{field}'"

        # 2. Total amount positivo
        try:
            amount = float(record["total_amount"])
            if amount <= 0:
                return False, f"Monto inválido (debe ser > 0): {amount}"
            if amount > 100000:
                return False, f"Monto sospechosamente alto (> 100k): {amount}"
        except (ValueError, TypeError):
            return False, f"Valor no numérico en total_amount: {record.get('total_amount')}"

        # 3. Status válido
        valid_statuses = {"COMPLETED", "PENDING", "CANCELLED", "SHIPPED"}
        if str(record["status"]).upper() not in valid_statuses:
            return False, f"Estado desconocido o inválido: '{record.get('status')}'"

        # 4. Formato de fecha YYYY-MM-DD
        try:
            datetime.strptime(str(record["order_date"]), "%Y-%m-%d")
        except ValueError:
            return False, f"Formato de fecha inválido (esperado YYYY-MM-DD): '{record.get('order_date')}'"

        return True, None

    def validate_customer(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Valida un cliente según reglas de formato y unicidad."""
        cust_id = record.get("customer_id")
        if not cust_id:
            return False, "customer_id ausente"

        email = record.get("email")
        if not email or not self.email_regex.match(str(email)):
            return False, f"Email inválido o malformado: '{email}'"

        return True, None

    def validate_product(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Valida un producto según reglas de catálogo y precios."""
        prod_id = record.get("product_id")
        if not prod_id:
            return False, "product_id ausente"

        name = record.get("product_name")
        if not name or not str(name).strip():
            return False, "product_name ausente o vacío"

        try:
            price = float(record.get("unit_price", 0))
            if price <= 0:
                return False, f"unit_price inválido (debe ser > 0): {price}"
        except (ValueError, TypeError):
            return False, f"unit_price no numérico: {record.get('unit_price')}"

        return True, None

    def validate_return(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Valida un registro de devolución."""
        ret_id = record.get("return_id")
        order_id = record.get("order_id")
        if not ret_id or not order_id:
            return False, "return_id u order_id ausente"

        try:
            refund = float(record.get("refund_amount", 0))
            if refund < 0:
                return False, f"refund_amount negativo: {refund}"
        except (ValueError, TypeError):
            return False, f"refund_amount no numérico: {record.get('refund_amount')}"

        return True, None

    def partition_records(
        self, records: List[Dict[str, Any]], entity_type: str = "orders"
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], ValidationReport]:
        """Segrega una lista de registros en válidos y en cuarentena con sus motivos."""
        valid_list: List[Dict[str, Any]] = []
        quarantine_list: List[Dict[str, Any]] = []
        reasons_summary: Dict[str, int] = {}

        validators = {
            "orders": self.validate_order,
            "customers": self.validate_customer,
            "products": self.validate_product,
            "returns": self.validate_return,
        }
        validator_func = validators.get(entity_type, self.validate_order)

        for rec in records:
            is_valid, reason = validator_func(rec)
            if is_valid:
                valid_list.append(rec)
            else:
                reason_str = reason or "Regla no superada"
                reasons_summary[reason_str] = reasons_summary.get(reason_str, 0) + 1

                # Clasificación de código de error
                error_code = "ERR_VALIDATION_FAILED"
                if "ausente" in reason_str.lower() or "vacío" in reason_str.lower():
                    error_code = "ERR_NULL_KEY"
                elif "monto inválido" in reason_str.lower() or "negativo" in reason_str.lower():
                    error_code = "ERR_NEGATIVE_AMOUNT"
                elif "estado desconocido" in reason_str.lower():
                    error_code = "ERR_INVALID_STATUS"
                elif "fecha" in reason_str.lower():
                    error_code = "ERR_INVALID_DATE"
                elif "email" in reason_str.lower():
                    error_code = "ERR_INVALID_EMAIL"

                quarantine_rec = {
                    "rejected_at": datetime.now(timezone.utc).isoformat(),
                    "entity_type": entity_type,
                    "error_code": error_code,
                    "quarantine_reason": reason_str,
                    "raw_record": rec,
                }
                quarantine_list.append(quarantine_rec)

        total = len(records)
        q_count = len(quarantine_list)
        q_rate = round(q_count / total, 4) if total > 0 else 0.0
        passed = q_rate <= 0.15  # Umbral del 15%

        report = ValidationReport(
            total_records=total,
            valid_records_count=len(valid_list),
            quarantine_records_count=q_count,
            quarantine_rate=q_rate,
            passed_threshold=passed,
            reasons_summary=reasons_summary,
        )

        logger.info(
            f"Validación de calidad completada para {entity_type}: "
            f"{len(valid_list)} válidos, {q_count} en cuarentena ({q_rate * 100:.1f}%)"
        )
        return valid_list, quarantine_list, report

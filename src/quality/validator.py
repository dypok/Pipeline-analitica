"""Validador de calidad de datos y segregación a Quarantine (US-08).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Permite evaluar reglas de negocio declarativas desde JSON, segregar filas corruptas
a la zona Quarantine en MinIO enriquecidas con metadatos técnicos, y soportar
advertencias (warnings no críticos) que preservan el registro para la capa Silver.
Consumible tanto en Python puro como dentro de jobs de PySpark.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Tuple

try:
    from src.common.logging_utils import get_logger
    logger = get_logger("DataQualityValidator")
except Exception:
    import logging
    logger = logging.getLogger("DataQualityValidator")

# Ruta por defecto al archivo de reglas declarativas
DEFAULT_RULES_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "quality_rules" / "retail_rules.json"


@dataclass
class ValidationResult:
    """Resultado de la evaluación individual de un registro."""

    is_valid: bool
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    warnings: List[Dict[str, str]] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Reporte agregado de la validación de calidad para un lote de datos."""

    total_records: int
    valid_records_count: int
    quarantine_records_count: int
    quarantine_rate: float
    passed_threshold: bool
    reasons_summary: Dict[str, int] = field(default_factory=dict)
    warnings_count: int = 0
    warnings_summary: Dict[str, int] = field(default_factory=dict)
    error_codes_summary: Dict[str, int] = field(default_factory=dict)


class DataQualityValidator:
    """Aplica reglas de negocio y calidad para segregar datos a Silver o Quarantine."""

    def __init__(
        self,
        rules: Optional[Dict[str, Any]] = None,
        rules_path: Optional[str | Path] = None,
    ):
        self.rules_path = Path(rules_path) if rules_path else DEFAULT_RULES_PATH
        loaded = self._load_rules()

        if rules:
            self.rules = rules
        elif loaded:
            self.rules = loaded
        else:
            try:
                from src.common.config import get_settings
                self.rules = get_settings().quality_rules
            except Exception:
                self.rules = {}

        # Configuración general
        settings_cfg = self.rules.get("settings", {}) if isinstance(self.rules, dict) else {}
        self.max_quarantine_rate = float(settings_cfg.get("max_quarantine_rate", 0.15))
        self.strict_mode = bool(settings_cfg.get("strict_mode", False))

        # Cache de regex compilados
        self._regex_cache: Dict[str, re.Pattern] = {
            "email": re.compile(r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"),
            "date": re.compile(r"^\d{4}-\d{2}-\d{2}$"),
            "phone": re.compile(r"^\+?[0-9]{7,15}$"),
        }

    def _load_rules(self) -> Dict[str, Any]:
        """Carga las reglas declarativas desde el archivo JSON si existe."""
        if self.rules_path.is_file():
            try:
                with open(self.rules_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    logger.info(f"Reglas de calidad cargadas exitosamente desde {self.rules_path}")
                    return data
            except Exception as e:
                logger.warning(f"No se pudo cargar el archivo de reglas {self.rules_path}: {e}")
        return {}

    def _get_entity_rules(self, entity_type: str) -> Dict[str, Any]:
        """Obtiene la sección de reglas específica para una entidad."""
        entities = self.rules.get("entities", {})
        if entity_type in entities:
            return entities[entity_type]
        # Fallback a compatibilidad con settings.yaml
        return self.rules.get(entity_type, {})

    # -------------------------------------------------------------------------
    # Métodos de validación detallada por entidad
    # -------------------------------------------------------------------------
    def validate_order_detailed(self, record: Dict[str, Any]) -> ValidationResult:
        """Valida una orden de compra retornando un ValidationResult estructurado."""
        rules = self._get_entity_rules("orders")
        warnings: List[Dict[str, str]] = []

        # 1. Campos obligatorios
        required = rules.get(
            "required_fields",
            ["order_id", "customer_id", "order_date", "total_amount", "status"],
        )
        for field_name in required:
            val = record.get(field_name)
            if val is None or (isinstance(val, str) and not val.strip()):
                return ValidationResult(
                    is_valid=False,
                    error_code="ERR_NULL_KEY",
                    error_message=f"Campo obligatorio ausente o vacío: '{field_name}'",
                )

        # 2. Total amount (numérico y dentro de rango)
        try:
            amount = float(record["total_amount"])
            if math.isnan(amount) or math.isinf(amount):
                return ValidationResult(
                    is_valid=False,
                    error_code="ERR_INVALID_TYPE",
                    error_message=f"Valor numérico no válido (NaN/Inf) en total_amount: {record.get('total_amount')}",
                )
        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_TYPE",
                error_message=f"Valor no numérico en total_amount: {record.get('total_amount')}",
            )

        if amount <= 0:
            return ValidationResult(
                is_valid=False,
                error_code="ERR_NEGATIVE_AMOUNT",
                error_message=f"Monto inválido (debe ser > 0): {amount}",
            )
        if amount > 100000:
            return ValidationResult(
                is_valid=False,
                error_code="ERR_AMOUNT_OUT_OF_BOUNDS",
                error_message=f"Monto sospechosamente alto (> 100k): {amount}",
            )

        # 3. Status válido
        valid_statuses = set(
            s.upper() for s in rules.get("valid_statuses", ["COMPLETED", "PENDING", "CANCELLED", "SHIPPED", "PROCESSING", "DELIVERED"])
        )
        status_val = str(record.get("status", "")).strip().upper()
        if status_val not in valid_statuses:
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_STATUS",
                error_message=f"Estado desconocido o inválido: '{record.get('status')}'",
            )

        # 4. Formato de fecha YYYY-MM-DD
        date_str = str(record.get("order_date", "")).strip()
        if not self._regex_cache["date"].match(date_str):
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_DATE",
                error_message=f"Formato de fecha inválido (esperado YYYY-MM-DD): '{date_str}'",
            )
        try:
            datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_DATE",
                error_message=f"Fecha de calendario inexistente o inválida: '{date_str}'",
            )

        # 5. Validación opcional de quantity si viene presente
        if "quantity" in record and record["quantity"] is not None:
            try:
                qty = int(record["quantity"])
                if qty <= 0:
                    return ValidationResult(
                        is_valid=False,
                        error_code="ERR_INVALID_QUANTITY",
                        error_message=f"quantity debe ser mayor a 0: {qty}",
                    )
            except (ValueError, TypeError):
                return ValidationResult(
                    is_valid=False,
                    error_code="ERR_INVALID_TYPE",
                    error_message=f"quantity no es un entero válido: {record.get('quantity')}",
                )

        # 6. Validación opcional de unit_price si viene presente
        if "unit_price" in record and record["unit_price"] is not None:
            try:
                price = float(record["unit_price"])
                if math.isnan(price) or math.isinf(price) or price <= 0:
                    return ValidationResult(
                        is_valid=False,
                        error_code="ERR_NEGATIVE_PRICE",
                        error_message=f"unit_price debe ser mayor a 0: {price}",
                    )
            except (ValueError, TypeError):
                return ValidationResult(
                    is_valid=False,
                    error_code="ERR_INVALID_TYPE",
                    error_message=f"unit_price no es numérico: {record.get('unit_price')}",
                )

        # 7. Reglas de advertencia (Warnings no críticos - Prueba T08.2)
        warning_cfg = rules.get("warning_rules", {})
        high_val_thresh = warning_cfg.get("total_amount", {}).get("high_value_threshold", 5000.0)
        if amount > high_val_thresh:
            warnings.append({
                "warning_code": "WARN_HIGH_VALUE_ORDER",
                "message": f"Orden de alto valor (${amount:.2f} > ${high_val_thresh:.2f})",
            })

        return ValidationResult(is_valid=True, warnings=warnings)

    def validate_customer_detailed(self, record: Dict[str, Any]) -> ValidationResult:
        """Valida un cliente retornando un ValidationResult estructurado."""
        rules = self._get_entity_rules("customers")
        warnings: List[Dict[str, str]] = []

        # 1. Campos obligatorios
        cust_id = record.get("customer_id")
        if not cust_id or not str(cust_id).strip():
            return ValidationResult(
                is_valid=False,
                error_code="ERR_NULL_KEY",
                error_message="customer_id ausente o vacío",
            )

        email = record.get("email")
        if not email or not str(email).strip():
            return ValidationResult(
                is_valid=False,
                error_code="ERR_NULL_KEY",
                error_message="email ausente o vacío",
            )

        email_str = str(email).strip()
        if not self._regex_cache["email"].match(email_str):
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_EMAIL",
                error_message=f"Email inválido o malformado: '{email}'",
            )

        # 2. Advertencias no críticas (Prueba T08.2: formato de teléfono)
        phone = record.get("phone")
        if phone:
            phone_str = str(phone).strip()
            if not self._regex_cache["phone"].match(phone_str):
                warnings.append({
                    "warning_code": "WARN_NON_STANDARD_PHONE",
                    "message": f"Formato de teléfono no estándar E.164: '{phone_str}'",
                })

        return ValidationResult(is_valid=True, warnings=warnings)

    def validate_product_detailed(self, record: Dict[str, Any]) -> ValidationResult:
        """Valida un producto de catálogo retornando un ValidationResult estructurado."""
        prod_id = record.get("product_id")
        if not prod_id or not str(prod_id).strip():
            return ValidationResult(
                is_valid=False,
                error_code="ERR_NULL_KEY",
                error_message="product_id ausente",
            )

        name = record.get("product_name")
        if not name or not str(name).strip():
            return ValidationResult(
                is_valid=False,
                error_code="ERR_NULL_KEY",
                error_message="product_name ausente o vacío",
            )

        try:
            price = float(record.get("unit_price", 0))
            if math.isnan(price) or math.isinf(price) or price <= 0:
                return ValidationResult(
                    is_valid=False,
                    error_code="ERR_NEGATIVE_PRICE",
                    error_message=f"unit_price inválido (debe ser > 0): {price}",
                )
        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_TYPE",
                error_message=f"unit_price no numérico: {record.get('unit_price')}",
            )

        if "cost_price" in record and record["cost_price"] is not None:
            try:
                cost = float(record["cost_price"])
                if math.isnan(cost) or math.isinf(cost) or cost < 0:
                    return ValidationResult(
                        is_valid=False,
                        error_code="ERR_NEGATIVE_COST",
                        error_message=f"cost_price negativo: {cost}",
                    )
            except (ValueError, TypeError):
                return ValidationResult(
                    is_valid=False,
                    error_code="ERR_INVALID_TYPE",
                    error_message=f"cost_price no numérico: {record.get('cost_price')}",
                )

        return ValidationResult(is_valid=True)

    def validate_return_detailed(self, record: Dict[str, Any]) -> ValidationResult:
        """Valida un registro de devolución retornando un ValidationResult estructurado."""
        ret_id = record.get("return_id")
        order_id = record.get("order_id")
        if not ret_id or not order_id:
            return ValidationResult(
                is_valid=False,
                error_code="ERR_NULL_KEY",
                error_message="return_id u order_id ausente",
            )

        try:
            refund = float(record.get("refund_amount", 0))
            if math.isnan(refund) or math.isinf(refund) or refund < 0:
                return ValidationResult(
                    is_valid=False,
                    error_code="ERR_NEGATIVE_REFUND",
                    error_message=f"refund_amount negativo: {refund}",
                )
        except (ValueError, TypeError):
            return ValidationResult(
                is_valid=False,
                error_code="ERR_INVALID_TYPE",
                error_message=f"refund_amount no numérico: {record.get('refund_amount')}",
            )

        return ValidationResult(is_valid=True)

    # -------------------------------------------------------------------------
    # Métodos compatibles con interfaz previa (Tuple[bool, Optional[str]])
    # -------------------------------------------------------------------------
    def validate_order(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        res = self.validate_order_detailed(record)
        return res.is_valid, res.error_message

    def validate_customer(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        res = self.validate_customer_detailed(record)
        return res.is_valid, res.error_message

    def validate_product(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        res = self.validate_product_detailed(record)
        return res.is_valid, res.error_message

    def validate_return(self, record: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        res = self.validate_return_detailed(record)
        return res.is_valid, res.error_message

    # -------------------------------------------------------------------------
    # Segregación de lotes a Silver / Quarantine con enriquecimiento técnico
    # -------------------------------------------------------------------------
    def partition_records(
        self, records: List[Dict[str, Any]], entity_type: str = "orders"
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], ValidationReport]:
        """Segrega una lista de registros en válidos (Silver) y rechazados (Quarantine).

        - Si el registro es válido pero contiene advertencias no críticas, pasa a la lista
          válida con la metadata de warning adjunta (Prueba T08.2).
        - Si el registro falla una regla crítica, se envía a Quarantine con:
          `_error_code`, `_error_message`, `_rejected_at` y `_original_payload` (US-08).
        """
        valid_list: List[Dict[str, Any]] = []
        quarantine_list: List[Dict[str, Any]] = []
        reasons_summary: Dict[str, int] = {}
        error_codes_summary: Dict[str, int] = {}
        warnings_summary: Dict[str, int] = {}
        total_warnings = 0

        detailed_validators = {
            "orders": self.validate_order_detailed,
            "customers": self.validate_customer_detailed,
            "products": self.validate_product_detailed,
            "returns": self.validate_return_detailed,
        }
        validator_func = detailed_validators.get(entity_type, self.validate_order_detailed)

        now_utc = datetime.now(timezone.utc).isoformat()

        for rec in records:
            res: ValidationResult = validator_func(rec)

            if res.is_valid:
                enriched_rec = dict(rec)
                if res.warnings:
                    warning_codes = [w["warning_code"] for w in res.warnings]
                    enriched_rec["_quality_warnings"] = warning_codes
                    total_warnings += len(warning_codes)
                    for w in res.warnings:
                        code = w["warning_code"]
                        warnings_summary[code] = warnings_summary.get(code, 0) + 1
                valid_list.append(enriched_rec)
            else:
                reason_str = res.error_message or "Regla no superada"
                error_code = res.error_code or "ERR_VALIDATION_FAILED"

                reasons_summary[reason_str] = reasons_summary.get(reason_str, 0) + 1
                error_codes_summary[error_code] = error_codes_summary.get(error_code, 0) + 1

                # Envelope técnico estandarizado para la zona Quarantine (US-08)
                quarantine_rec = {
                    "_error_code": error_code,
                    "_error_message": reason_str,
                    "_rejected_at": now_utc,
                    "_original_payload": dict(rec),
                    "entity_type": entity_type,
                    # Retrocompatibilidad para scripts o tests previos
                    "error_code": error_code,
                    "quarantine_reason": reason_str,
                    "raw_record": rec,
                    "rejected_at": now_utc,
                }
                quarantine_list.append(quarantine_rec)

        total = len(records)
        q_count = len(quarantine_list)
        q_rate = round(q_count / total, 4) if total > 0 else 0.0
        passed = q_rate <= self.max_quarantine_rate

        report = ValidationReport(
            total_records=total,
            valid_records_count=len(valid_list),
            quarantine_records_count=q_count,
            quarantine_rate=q_rate,
            passed_threshold=passed,
            reasons_summary=reasons_summary,
            warnings_count=total_warnings,
            warnings_summary=warnings_summary,
            error_codes_summary=error_codes_summary,
        )

        logger.info(
            f"Validación de calidad ({entity_type}): {len(valid_list)} válidos ({total_warnings} warnings), "
            f"{q_count} en cuarentena ({q_rate * 100:.1f}% vs umbral {self.max_quarantine_rate * 100:.1f}%)"
        )
        return valid_list, quarantine_list, report

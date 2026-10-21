"""Catálogo de datos técnicos y de negocio de la plataforma Medallion (US-09).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Define esquemas, tipos, descripciones y capas del Lakehouse para OpenMetadata.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from src.governance.classification import ClassificationEngine


@dataclass
class ColumnMetadata:
    """Metadatos de una columna o atributo de un dataset."""

    name: str
    data_type: str
    description: str
    nullable: bool = True
    is_primary_key: bool = False
    classification: str = "PUBLIC"


@dataclass
class TableMetadata:
    """Metadatos de una tabla o dataset dentro del Lakehouse."""

    name: str
    layer: str  # LANDING, BRONZE, SILVER, GOLD, QUARANTINE
    storage_uri: str
    description: str
    columns: List[ColumnMetadata] = field(default_factory=list)
    owner: str = "Data Engineering Team"


class DataCatalog:
    """Catálogo unificado de esquemas y metadatos del Lakehouse."""

    def __init__(self, classification_engine: Optional[ClassificationEngine] = None):
        self.classifier = classification_engine or ClassificationEngine()
        self.tables: Dict[str, TableMetadata] = self._build_catalog()

    def _build_catalog(self) -> Dict[str, TableMetadata]:
        catalog: Dict[str, TableMetadata] = {}

        # 1. Capa Silver - Orders
        order_cols = [
            ColumnMetadata("order_id", "VARCHAR(64)", "Identificador único de la orden", False, True),
            ColumnMetadata("customer_id", "VARCHAR(64)", "Identificador del cliente", False, False),
            ColumnMetadata("order_date", "DATE", "Fecha de emisión de la orden (YYYY-MM-DD)", False),
            ColumnMetadata("total_amount", "DOUBLE", "Monto total monetario de la orden", False),
            ColumnMetadata("status", "VARCHAR(20)", "Estado de la orden (COMPLETED, PENDING, etc.)", False),
            ColumnMetadata("shipping_country", "VARCHAR(10)", "País de destino de envío", True),
            ColumnMetadata("category", "VARCHAR(50)", "Categoría del producto principal", True),
            ColumnMetadata("quantity", "INT", "Unidades compradas", False),
            ColumnMetadata("unit_price", "DOUBLE", "Precio unitario del ítem", False),
            ColumnMetadata("year", "INT", "Partición temporal anual", False),
            ColumnMetadata("month", "INT", "Partición temporal mensual", False),
        ]
        self._apply_classification("orders", order_cols)
        catalog["silver.orders"] = TableMetadata(
            name="silver.orders",
            layer="SILVER",
            storage_uri="s3://silver/retail/orders/",
            description="Órdenes de compra limpias, deduplicadas, tipadas y validadas en Parquet Snappy",
            columns=order_cols,
            owner="Esteban Padilla",
        )

        # 2. Capa Silver - Customers
        customer_cols = [
            ColumnMetadata("customer_id", "VARCHAR(64)", "Identificador único del cliente", False, True),
            ColumnMetadata("first_name", "VARCHAR(100)", "Nombre de pila del cliente", True),
            ColumnMetadata("last_name", "VARCHAR(100)", "Apellido del cliente", True),
            ColumnMetadata("email", "VARCHAR(150)", "Correo electrónico verificado", False),
            ColumnMetadata("phone", "VARCHAR(30)", "Número telefónico en formato estándar", True),
            ColumnMetadata("country", "VARCHAR(10)", "País de residencia del cliente", True),
            ColumnMetadata("city", "VARCHAR(100)", "Ciudad de residencia", True),
            ColumnMetadata("registration_date", "DATE", "Fecha de registro en la plataforma", False),
        ]
        self._apply_classification("customers", customer_cols)
        catalog["silver.customers"] = TableMetadata(
            name="silver.customers",
            layer="SILVER",
            storage_uri="s3://silver/retail/customers/",
            description="Catálogo de clientes con emails validados y PII identificado para gobierno",
            columns=customer_cols,
            owner="Esteban Padilla",
        )

        # 3. Capa Gold - daily_sales
        daily_sales_cols = [
            ColumnMetadata("order_date", "DATE", "Fecha de agregación analítica (grano primario)", False, True),
            ColumnMetadata("shipping_country", "VARCHAR(10)", "País de envío", False, True),
            ColumnMetadata("category", "VARCHAR(50)", "Categoría de producto", False, True),
            ColumnMetadata("total_revenue", "DOUBLE", "Ingresos brutos acumulados en órdenes COMPLETED", False),
            ColumnMetadata("orders_count", "INT", "Cantidad total de órdenes completadas", False),
            ColumnMetadata("units_sold", "INT", "Unidades totales vendidas", False),
            ColumnMetadata("avg_ticket", "DOUBLE", "Ticket promedio (total_revenue / orders_count)", False),
            ColumnMetadata("returns_count", "INT", "Total de órdenes devueltas", False),
            ColumnMetadata("refunded_amount", "DOUBLE", "Monto reembolsado en devoluciones", False),
        ]
        self._apply_classification("daily_sales", daily_sales_cols)
        catalog["gold.daily_sales"] = TableMetadata(
            name="gold.daily_sales",
            layer="GOLD",
            storage_uri="s3://gold/analytics/daily_sales/ & postgres.analytics.daily_sales_kpis",
            description="Modelo analítico dimensional de ventas diarias por país y categoría",
            columns=daily_sales_cols,
            owner="Esteban Padilla / Sergio",
        )

        # 4. Capa Gold - customer_360
        c360_cols = [
            ColumnMetadata("customer_id", "VARCHAR(64)", "Identificador del cliente", False, True),
            ColumnMetadata("country", "VARCHAR(10)", "País del cliente", True),
            ColumnMetadata("first_purchase_date", "DATE", "Fecha de la primera compra registrada", True),
            ColumnMetadata("last_purchase_date", "DATE", "Fecha de la última compra registrada", True),
            ColumnMetadata("total_orders", "INT", "Número total de órdenes emitidas", False),
            ColumnMetadata("lifetime_value", "DOUBLE", "Gasto histórico total acumulado (LTV)", False),
            ColumnMetadata("avg_order_value", "DOUBLE", "Monto promedio por orden", False),
            ColumnMetadata("favorite_category", "VARCHAR(50)", "Categoría con más unidades adquiridas", True),
        ]
        self._apply_classification("customer_360", c360_cols)
        catalog["gold.customer_360"] = TableMetadata(
            name="gold.customer_360",
            layer="GOLD",
            storage_uri="s3://gold/analytics/customer_360/",
            description="Modelo analítico 360 de clientes con métricas RFM y valor acumulado",
            columns=c360_cols,
            owner="Esteban Padilla / Dylan Gamero",
        )

        # 5. Capa Quarantine
        quarantine_cols = [
            ColumnMetadata("_error_code", "VARCHAR(50)", "Código de error técnico (ERR_NULL_KEY, etc.)", False),
            ColumnMetadata("_error_message", "TEXT", "Mensaje legible del motivo de rechazo", False),
            ColumnMetadata("_rejected_at", "TIMESTAMP", "Marca temporal UTC del descarte", False),
            ColumnMetadata("entity_type", "VARCHAR(30)", "Entidad de negocio evaluada", False),
            ColumnMetadata("_original_payload", "JSON", "Registro crudo original para trazabilidad y auditoría", False),
        ]
        catalog["quarantine.anomalies"] = TableMetadata(
            name="quarantine.anomalies",
            layer="QUARANTINE",
            storage_uri="s3://quarantine/rejected/",
            description="Registros corruptos o anómalos segregados por el motor de calidad de Sebastián",
            columns=quarantine_cols,
            owner="Sebastián Ortiz",
        )

        return catalog

    def _apply_classification(self, entity_name: str, cols: List[ColumnMetadata]):
        for col in cols:
            col.classification = self.classifier.get_classification(entity_name, col.name)

    def get_table(self, table_name: str) -> Optional[TableMetadata]:
        return self.tables.get(table_name)

    def list_tables(self) -> List[str]:
        return list(self.tables.keys())

    def export_openmetadata_tables(self) -> List[Dict[str, Any]]:
        """Exporta las entidades en la especificación JSON de OpenMetadata."""
        om_tables = []
        for name, meta in self.tables.items():
            om_cols = []
            for col in meta.columns:
                om_cols.append({
                    "name": col.name,
                    "dataType": col.data_type,
                    "description": col.description,
                    "tags": [{"tagFQN": f"Security.{col.classification}"}] if col.classification != "PUBLIC" else [],
                })
            om_tables.append({
                "name": name,
                "fullyQualifiedName": f"medallion_lakehouse.{meta.layer.lower()}.{name}",
                "description": meta.description,
                "columns": om_cols,
                "owner": meta.owner,
            })
        return om_tables

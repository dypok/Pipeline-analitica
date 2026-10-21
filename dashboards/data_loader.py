"""Carga unificada de datos analíticos desde la capa Gold.

Estrategia de carga con degradación graceful (contrato de integración en
``docs/contratos_integracion.md`` §7):

1. **PostgreSQL analítico** (recomendado): lee las tablas del esquema
   ``analytics`` (``daily_sales_kpis``, ``customer_360``, ``product_performance``,
   ``pipeline_execution_logs``, ``pipeline_task_metrics``).
2. **DuckDB sobre Parquet en MinIO**: cuando PostgreSQL no está disponible,
   consulta directamente los archivos ``s3://gold/analytics/<modelo>/*.parquet``.
3. **DuckDB sobre Parquet local**: si MinIO tampoco está disponible, lee los
   Parquet mock desde ``DASHBOARDS_GOLD_PATH`` (por defecto ``./data/mock/gold``).

Esta degradación permite a Sergio maquetar los dashboards en paralelo desde el
inicio usando los datos mock de Luis, y cambiar a producción sin tocar la UI.
"""

from __future__ import annotations

import os
import socket
from urllib.parse import urlparse
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional

import pandas as pd
from dotenv import load_dotenv

load_dotenv()


def _is_service_reachable(host: str, port: int, timeout: float = 0.3) -> bool:
    """Verifica rápidamente si un servicio de red está escuchando en el puerto dado.

    Evita esperas largas de SQLAlchemy/DuckDB cuando el servicio está caído
    (degradación graceful inmediata en modo local).
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def _normalize_daily_sales(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza un DataFrame al esquema del data mart §4.1 tolerando el Gold US-07.

    Acepta indistintamente el modelo ``analytics.daily_sales_kpis`` (kpi_date,
    total_orders, ...) o el Gold US-07 (order_date, orders_count, ...). Crea
    las columnas faltantes a partir de las disponibles para que las vistas del
    dashboard funcionen sin importar el esquema de origen. Idempotente.
    """
    if df.empty:
        return df
    out = df.copy()

    # Fechas: kpi_date <-> order_date
    if "order_date" in out.columns and "kpi_date" not in out.columns:
        out["kpi_date"] = pd.to_datetime(out["order_date"])
    elif "kpi_date" in out.columns:
        out["kpi_date"] = pd.to_datetime(out["kpi_date"])
        if "order_date" not in out.columns:
            out["order_date"] = out["kpi_date"]

    # Órdenes: total_orders <-> orders_count
    if "orders_count" in out.columns and "total_orders" not in out.columns:
        out["total_orders"] = out["orders_count"]
    elif "total_orders" in out.columns and "orders_count" not in out.columns:
        out["orders_count"] = out["total_orders"]

    # completed/cancelled solo existen en el data mart; en el Gold US-07 se
    # aproximan (completed = todas; cancelled = returns_count).
    if "completed_orders" not in out.columns:
        out["completed_orders"] = out.get("total_orders", out.get("orders_count", 0))
    if "cancelled_orders" not in out.columns:
        out["cancelled_orders"] = out.get("returns_count", 0)
    if "returns_count" not in out.columns:
        out["returns_count"] = out.get("cancelled_orders", 0)
    if "distinct_customers" not in out.columns:
        out["distinct_customers"] = out.get("total_orders", out.get("orders_count", 0))

    # Ticket promedio: average_ticket <-> avg_ticket
    if "avg_ticket" in out.columns and "average_ticket" not in out.columns:
        out["average_ticket"] = out["avg_ticket"]
    elif "average_ticket" in out.columns and "avg_ticket" not in out.columns:
        out["avg_ticket"] = out["average_ticket"]

    # Categoría: top_category <-> category
    if "category" in out.columns and "top_category" not in out.columns:
        out["top_category"] = out["category"]
    elif "top_category" in out.columns and "category" not in out.columns:
        out["category"] = out["top_category"]

    # País: country <-> shipping_country
    if "shipping_country" in out.columns and "country" not in out.columns:
        out["country"] = out["shipping_country"]
    elif "country" in out.columns and "shipping_country" not in out.columns:
        out["shipping_country"] = out["country"]

    return out


@dataclass(frozen=True)
class LoaderSource:
    """Identifica qué fuente de datos se usó en la última carga."""

    backend: str  # "postgres" | "minio" | "local_parquet"
    detail: str


class GoldDataLoader:
    """Cargador de datos Gold con degradación graceful entre backends."""

    def __init__(
        self,
        pg_url: Optional[str] = None,
        minio_endpoint: Optional[str] = None,
        minio_access_key: Optional[str] = None,
        minio_secret_key: Optional[str] = None,
        local_gold_path: Optional[str] = None,
        gold_bucket: str = "gold",
    ) -> None:
        self.pg_url = pg_url or os.getenv(
            "HOST_ANALYTICS_DB_URL",
            "postgresql+psycopg2://medallion_user:medallion_secret@localhost:5433/analytics_db",
        )
        self.minio_endpoint = minio_endpoint or os.getenv("MINIO_HOST_ENDPOINT", "http://localhost:9000")
        self.minio_access_key = minio_access_key or os.getenv("MINIO_ROOT_USER", "minioadmin")
        self.minio_secret_key = minio_secret_key or os.getenv("MINIO_ROOT_PASSWORD", "miniopassword123")
        self.local_gold_path = local_gold_path or os.getenv("DASHBOARDS_GOLD_PATH", "./data/mock/gold")
        self.gold_bucket = os.getenv("MINIO_BUCKET_GOLD", gold_bucket)
        self._source: Optional[LoaderSource] = None

    @property
    def source(self) -> Optional[LoaderSource]:
        """Retorna la última fuente de datos utilizada (para mostrar en la UI)."""
        return self._source

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------
    def load_daily_sales(self) -> pd.DataFrame:
        """Carga el modelo ``daily_sales_kpis`` (data mart §4.1, 1 fila/día).

        NOTA: Lee el **data mart** ``analytics.daily_sales_kpis`` (§4.1), que es
        el modelo pre-agregado para dashboards. No confundir con el Gold US-07
        ``daily_sales`` (grano fecha×país×categoría, §7.2), que consume el
        chatbot de Dylan vía DuckDB. En local el mock es ``daily_sales_kpis.parquet``.
        """
        df = self._load_model(
            sql_table="analytics.daily_sales_kpis",
            s3_prefix="analytics/daily_sales_kpis",
            local_file="daily_sales_kpis.parquet",
            columns=[
                "kpi_date", "total_orders", "completed_orders", "cancelled_orders",
                "total_revenue", "average_ticket", "distinct_customers", "top_category",
            ],
        )
        return _normalize_daily_sales(df)

    def load_customer_360(self) -> pd.DataFrame:
        """Carga el modelo ``customer_360`` (visión 360° del cliente).

        TODO(US-07): en cuanto Esteban publique el modelo en Gold/PostgreSQL,
        validar que el esquema coincide con el contrato:
            customer_id, first_purchase_date, last_purchase_date, total_orders,
            lifetime_value, avg_order_value, favorite_category
        """
        df = self._load_model(
            sql_table="analytics.customer_360",
            s3_prefix="analytics/customer_360",
            local_file="customer_360.parquet",
            columns=[
                "customer_id", "first_purchase_date", "last_purchase_date",
                "total_orders", "lifetime_value", "avg_order_value", "favorite_category",
            ],
        )
        if not df.empty:
            # Tolerancia a aliases de esquema alternativos (data lake vs data mart).
            df = df.copy()
            aliases = {
                "total_lifetime_orders": "total_orders",
                "total_lifetime_spend": "lifetime_value",
                "average_order_value": "avg_order_value",
                "first_order_date": "first_purchase_date",
                "last_order_date": "last_purchase_date",
            }
            for src, dst in aliases.items():
                if src in df.columns and dst not in df.columns:
                    df[dst] = df[src]
        for col in ("first_purchase_date", "last_purchase_date"):
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors="coerce")
        return df

    def load_product_performance(self) -> pd.DataFrame:
        """Carga el modelo ``product_performance`` (desempeño por producto).

        TODO(US-07): alinearse con el contrato:
            product_id, units_sold, gross_revenue, return_rate, margin
        """
        df = self._load_model(
            sql_table="analytics.product_performance",
            s3_prefix="analytics/product_performance",
            local_file="product_performance.parquet",
            columns=[
                "product_id", "units_sold", "gross_revenue", "return_rate", "margin",
            ],
        )
        return df

    def load_pipeline_executions(self) -> pd.DataFrame:
        """Carga los logs de ejecución del pipeline (``pipeline_execution_logs``).

        Usado por la pestaña "Salud del Pipeline" (US-15).
        """
        return self._load_model(
            sql_table="analytics.pipeline_execution_logs",
            s3_prefix=None,  # No hay parquet mock para logs operacionales
            local_file=None,
            columns=[
                "execution_id", "pipeline_name", "layer", "status",
                "records_processed", "records_quarantined", "finished_at", "error_message",
            ],
        )

    def load_task_metrics(self) -> pd.DataFrame:
        """Carga las métricas operativas por tarea (``pipeline_task_metrics``).

        Usado por la pestaña "Salud del Pipeline" (US-15).
        """
        return self._load_model(
            sql_table="analytics.pipeline_task_metrics",
            s3_prefix=None,
            local_file=None,
            columns=[
                "run_id", "task_id", "pipeline_name", "layer", "status",
                "duration_seconds", "records_processed", "records_quarantined", "recorded_at",
            ],
        )

    def load_quarantine_records(self) -> pd.DataFrame:
        """Carga los registros en Quarantine (US-15 / P04).

        Contrato (``docs/contratos_integracion.md`` §3.1):
            rejected_at, entity_type, quarantine_reason, raw_record

        TODO: en cuanto Sebastián publique el escritor de Quarantine, integrar
        la lectura desde MinIO (``s3://quarantine/rejected/<dataset>/...``) o
        desde una tabla ``analytics.quarantine_records`` si se define.
        """
        # Esqueleto: por ahora devuelve un DataFrame vacío con el esquema esperado.
        # Cuando esté disponible la fuente real, ramificar aquí.
        return pd.DataFrame(
            columns=["rejected_at", "entity_type", "quarantine_reason", "raw_record"]
        )

    # ------------------------------------------------------------------
    # Estrategia de degradación
    # ------------------------------------------------------------------
    def _load_model(
        self,
        sql_table: str,
        s3_prefix: Optional[str],
        local_file: Optional[str],
        columns: list[str],
    ) -> pd.DataFrame:
        """Intenta cargar el modelo desde PostgreSQL → MinIO → Local Parquet."""
        # 1. PostgreSQL (recomendado)
        df = self._try_postgres(sql_table, columns)
        if df is not None and not df.empty:
            self._source = LoaderSource("postgres", sql_table)
            return df

        # 2. MinIO vía DuckDB
        if s3_prefix is not None:
            df = self._try_minio_parquet(s3_prefix, columns)
            if df is not None and not df.empty:
                self._source = LoaderSource("minio", f"s3://{self.gold_bucket}/{s3_prefix}/")
                return df

        # 3. Parquet local (mock)
        if local_file is not None:
            df = self._try_local_parquet(local_file, columns)
            if df is not None and not df.empty:
                self._source = LoaderSource("local_parquet", local_file)
                return df

        # 4. Sin datos disponibles
        # No reseteamos ``self._source`` para preservar la fuente del último
        # modelo Gold cargado con éxito. Así la UI muestra la fuente real de
        # los datos analíticos aunque las tablas operacionales (executions,
        # task_metrics, quarantine) no estén disponibles.
        return pd.DataFrame(columns=columns)

    def _try_postgres(self, table: str, columns: list[str]) -> Optional[pd.DataFrame]:
        # Verificación rápida de disponibilidad de socket antes de invocar
        # SQLAlchemy (evita esperas largas cuando Postgres está caído).
        try:
            parsed = urlparse(self.pg_url.replace("postgresql+psycopg2://", "http://"))
            host = parsed.hostname or "localhost"
            port = parsed.port or 5433
            if not _is_service_reachable(host, port, timeout=0.3):
                return None
        except Exception:
            pass

        try:
            from sqlalchemy import create_engine, text
        except ImportError:
            return None
        try:
            engine = create_engine(
                self.pg_url,
                pool_pre_ping=True,
                future=True,
                connect_args={"connect_timeout": 2},
            )
            with engine.connect() as conn:
                # SELECT * para tolerar columnas extra que el esquema Gold pueda
                # añadir (country, category, product_name, etc.). Las vistas
                # comprueban existencia de columnas antes de usarlas.
                df = pd.read_sql(text(f"SELECT * FROM {table}"), conn)
            return df
        except Exception:
            # PostgreSQL no disponible (modo local / MinIO caído). Degradación.
            return None

    def _try_minio_parquet(self, s3_prefix: str, columns: list[str]) -> Optional[pd.DataFrame]:
        # Verificación rápida de socket para MinIO (evita timeouts de DuckDB).
        try:
            parsed = urlparse(self.minio_endpoint)
            host = parsed.hostname or "localhost"
            port = parsed.port or 9000
            if not _is_service_reachable(host, port, timeout=0.3):
                return None
        except Exception:
            pass

        try:
            import duckdb
        except ImportError:
            return None
        try:
            con = duckdb.connect()
            con.execute("INSTALL httpfs; LOAD httpfs;")
            host = self.minio_endpoint.replace("http://", "").replace("https://", "")
            con.execute(f"SET s3_endpoint='{host}';")
            con.execute(f"SET s3_access_key_id='{self.minio_access_key}';")
            con.execute(f"SET s3_secret_access_key='{self.minio_secret_key}';")
            con.execute("SET s3_use_ssl=false;")
            con.execute("SET s3_url_style='path';")
            df = con.execute(
                f"SELECT * FROM 's3://{self.gold_bucket}/{s3_prefix}/*.parquet'"
            ).df()
            # Devolver todas las columnas disponibles; las vistas comprueban
            # existencia antes de usar cada columna.
            return df
        except Exception:
            return None

    def _try_local_parquet(self, filename: str, columns: list[str]) -> Optional[pd.DataFrame]:
        from pathlib import Path
        path = Path(self.local_gold_path) / filename
        if not path.exists():
            return None
        try:
            # Devolver todas las columnas disponibles; las vistas comprueban
            # existencia antes de usar cada columna.
            return pd.read_parquet(path)
        except Exception:
            return None


@lru_cache(maxsize=1)
def get_loader() -> GoldDataLoader:
    """Factory singleton del cargador (compartido por todas las vistas)."""
    return GoldDataLoader()

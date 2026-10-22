import logging
from pathlib import Path
import time
from typing import Any
import duckdb
from app.backend.core.config import settings

logger = logging.getLogger(__name__)


class DuckDBClient:
    """Cliente y motor de consulta analítico basado en DuckDB.
    Monta vistas SQL sobre los archivos Parquet de la capa Gold.
    """

    def __init__(self, data_path: str | None = None):
        self.data_path = Path(data_path or settings.GOLD_DATA_PATH)
        if not self.data_path.is_absolute():
            self.data_path = settings.BASE_DIR / self.data_path

        self._conn: duckdb.DuckDBPyConnection | None = None
        self._registered_views: set[str] = set()
        self.connect()

    def connect(self) -> None:
        """Inicializa la conexión en memoria y registra las vistas Gold."""
        self._conn = duckdb.connect(database=":memory:")
        self.refresh_views()

    def refresh_views(self) -> None:
        """Crea o actualiza las vistas SQL hacia los archivos Parquet disponibles."""
        if not self._conn:
            return

        views_mapping = {
            "v_daily_sales": self.data_path / "daily_sales.parquet",
            "v_customer_360": self.data_path / "customer_360.parquet",
            "v_product_performance": self.data_path / "product_performance.parquet",
            "v_pipeline_execution_logs": self.data_path / "pipeline_execution_logs.parquet",
        }

        for view_name, parquet_path in views_mapping.items():
            if parquet_path.exists():
                escaped_path = str(parquet_path).replace("'", "''")
                self._conn.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{escaped_path}')"
                )
                self._registered_views.add(view_name)
                logger.info("Vista registrada exitosamente: %s -> %s", view_name, parquet_path)
            else:
                logger.warning("Archivo Parquet no encontrado para vista: %s (%s)", view_name, parquet_path)

    @property
    def registered_views(self) -> set[str]:
        return self._registered_views

    def execute_query(self, sql: str, max_rows: int | None = None) -> dict[str, Any]:
        """Ejecuta una consulta SQL en DuckDB y retorna resultados formateados y métricas de rendimiento."""
        if not self._conn:
            self.connect()

        max_limit = max_rows or settings.MAX_QUERY_ROWS
        start_time = time.perf_counter()

        try:
            rel = self._conn.execute(sql)
            df = rel.df()
            elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)

            total_found = len(df)
            if total_found > max_limit:
                df = df.head(max_limit)

            columns = list(df.columns)
            records = df.to_dict(orient="records")

            return {
                "success": True,
                "columns": columns,
                "data": records,
                "row_count": len(records),
                "total_rows_matched": total_found,
                "execution_time_ms": elapsed_ms,
                "truncated": total_found > max_limit,
            }
        except Exception as e:
            elapsed_ms = round((time.perf_counter() - start_time) * 1000, 2)
            logger.error("Error ejecutando SQL en DuckDB: %s. SQL: %s", str(e), sql)
            return {
                "success": False,
                "error": str(e),
                "columns": [],
                "data": [],
                "row_count": 0,
                "total_rows_matched": 0,
                "execution_time_ms": elapsed_ms,
                "truncated": False,
            }

    def get_schemas(self) -> dict[str, list[dict[str, str]]]:
        """Obtiene la metadata de columnas y tipos de datos de las vistas registradas."""
        if not self._conn:
            self.connect()

        schemas = {}
        for view in self._registered_views:
            res = self._conn.execute(f"DESCRIBE {view}").fetchall()
            schemas[view] = [{"name": row[0], "type": row[1]} for row in res]
        return schemas

    def is_healthy(self) -> bool:
        """Comprueba que el motor esté operativo y pueda ejecutar consultas."""
        try:
            res = self.execute_query("SELECT 1 AS status")
            return res.get("success", False) and len(res.get("data", [])) > 0
        except Exception:
            return False

    def get_daily_sales_summary(
        self,
        start_date: str | None = None,
        end_date: str | None = None,
        country: str | None = None,
        category: str | None = None,
    ) -> dict[str, Any]:
        """Obtiene KPIs agregados y serie temporal desde v_daily_sales."""
        conditions = []
        if start_date:
            conditions.append(f"order_date >= '{start_date}'")
        if end_date:
            conditions.append(f"order_date <= '{end_date}'")
        if country:
            country_clean = country.strip()
            aliases = {
                "colombia": "CO", "méxico": "MX", "mexico": "MX",
                "argentina": "AR", "chile": "CL", "perú": "PE", "peru": "PE",
                "estados unidos": "US", "españa": "ES", "espana": "ES",
                "co": "Colombia", "mx": "México", "ar": "Argentina",
                "cl": "Chile", "pe": "Perú", "us": "Estados Unidos", "es": "España"
            }
            norm = aliases.get(country_clean.lower())
            if norm:
                conditions.append(f"(shipping_country = '{country_clean}' OR shipping_country = '{norm}')")
            else:
                conditions.append(f"shipping_country = '{country_clean}'")
        if category:
            conditions.append(f"category = '{category}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # Consulta de KPIs globales
        kpi_sql = f"""
            SELECT
                ROUND(COALESCE(SUM(total_revenue), 0), 2) AS total_revenue,
                COALESCE(SUM(orders_count), 0) AS total_orders,
                COALESCE(SUM(units_sold), 0) AS total_units_sold,
                COALESCE(SUM(returns_count), 0) AS total_returns,
                ROUND(COALESCE(SUM(refunded_amount), 0), 2) AS total_refunded,
                ROUND(
                    CASE WHEN COALESCE(SUM(orders_count), 0) > 0
                         THEN SUM(total_revenue) / SUM(orders_count)
                         ELSE 0 END, 2
                ) AS avg_ticket
            FROM v_daily_sales
            {where_clause}
        """

        # Consulta de serie de tiempo
        trend_sql = f"""
            SELECT
                order_date,
                ROUND(SUM(total_revenue), 2) AS daily_revenue,
                SUM(orders_count) AS daily_orders
            FROM v_daily_sales
            {where_clause}
            GROUP BY order_date
            ORDER BY order_date ASC
        """

        kpis = self.execute_query(kpi_sql)
        trend = self.execute_query(trend_sql)

        return {
            "kpis": kpis["data"][0] if kpis["data"] else {},
            "trend": trend["data"],
            "execution_time_ms": kpis["execution_time_ms"] + trend["execution_time_ms"],
        }


# Instancia singleton para reuso en la aplicación
duckdb_engine = DuckDBClient()

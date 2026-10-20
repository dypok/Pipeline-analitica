import logging
from typing import Any
from fastapi import APIRouter, HTTPException, Query, status

from app.backend.api.models import (
    ChatbotAskRequest,
    ChatbotAskResponse,
    ExecuteQueryRequest,
    ExecuteQueryResponse,
    HealthResponse,
)
from app.backend.core.config import settings
from app.backend.query_engine.duckdb_client import duckdb_engine
from app.backend.services.chatbot_service import chatbot_service
from app.backend.services.sql_guard import SQLGuard, SecurityException

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse, summary="Verificar estado de la API y DuckDB")
def get_health() -> HealthResponse:
    is_ok = duckdb_engine.is_healthy()
    return HealthResponse(
        status="healthy" if is_ok else "degraded",
        version=settings.VERSION,
        environment=settings.ENVIRONMENT,
        duckdb_connected=is_ok,
        registered_views=sorted(list(duckdb_engine.registered_views)),
    )


@router.get("/catalog/schemas", summary="Obtener esquemas y tipos de datos de las vistas Gold")
def get_catalog_schemas() -> dict[str, Any]:
    schemas = duckdb_engine.get_schemas()
    return {"success": True, "schemas": schemas}


@router.get("/metrics/daily-sales", summary="Métricas de ventas y serie temporal")
def get_daily_sales_metrics(
    start_date: str | None = Query(default=None, description="Fecha inicial YYYY-MM-DD"),
    end_date: str | None = Query(default=None, description="Fecha final YYYY-MM-DD"),
    country: str | None = Query(default=None, description="País de envío"),
    category: str | None = Query(default=None, description="Categoría de producto"),
) -> dict[str, Any]:
    return duckdb_engine.get_daily_sales_summary(
        start_date=start_date,
        end_date=end_date,
        country=country,
        category=category,
    )


@router.get("/metrics/top-products", summary="Ranking de productos por volumen o ingresos")
def get_top_products(
    limit: int = Query(default=10, ge=1, le=100, description="Cantidad de productos a retornar"),
    category: str | None = Query(default=None, description="Filtrar por categoría"),
) -> dict[str, Any]:
    where_clause = f"WHERE category = '{category}'" if category else ""
    sql = f"""
        SELECT
            product_id,
            product_name,
            category,
            units_sold,
            gross_revenue,
            returns_count,
            ROUND(return_rate * 100, 2) AS return_rate_pct,
            ROUND(CASE WHEN gross_revenue > 0 THEN (margin / gross_revenue) * 100 ELSE 0 END, 2) AS profit_margin_pct
        FROM v_product_performance
        {where_clause}
        ORDER BY units_sold DESC
        LIMIT {limit}
    """
    return duckdb_engine.execute_query(sql)


@router.get("/metrics/customer-summary", summary="Resumen analítico de clientes (Customer 360)")
def get_customer_summary() -> dict[str, Any]:
    sql = """
        SELECT
            country,
            COUNT(*) AS total_customers,
            ROUND(AVG(lifetime_value), 2) AS avg_lifetime_value,
            ROUND(AVG(avg_order_value), 2) AS avg_ticket,
            ROUND(AVG(total_orders), 1) AS avg_orders_per_customer
        FROM v_customer_360
        GROUP BY country
        ORDER BY total_customers DESC
    """
    return duckdb_engine.execute_query(sql)


@router.post("/query/execute", response_model=ExecuteQueryResponse, summary="Ejecutar consulta SQL segura")
def execute_query(payload: ExecuteQueryRequest) -> ExecuteQueryResponse:
    # 1. Validación estricta con SQLGuard
    try:
        sanitized_sql = SQLGuard.validate_and_sanitize(payload.sql, max_limit=payload.max_rows)
    except SecurityException as sec_ex:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Consulta no permitida por seguridad: {str(sec_ex)}",
        )

    # 2. Ejecución sobre DuckDB
    res = duckdb_engine.execute_query(sanitized_sql, max_rows=payload.max_rows)

    if not res.get("success"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error en sintaxis o ejecución SQL: {res.get('error')}",
        )

    return ExecuteQueryResponse(
        success=True,
        columns=res.get("columns", []),
        data=res.get("data", []),
        row_count=res.get("row_count", 0),
        total_rows_matched=res.get("total_rows_matched", 0),
        execution_time_ms=res.get("execution_time_ms", 0.0),
        truncated=res.get("truncated", False),
    )


@router.post("/chatbot/ask", response_model=ChatbotAskResponse, summary="Consultar al Asistente IA")
def chatbot_ask(payload: ChatbotAskRequest) -> ChatbotAskResponse:
    res = chatbot_service.ask(payload.question, payload.dataset_hint)
    return ChatbotAskResponse(**res)

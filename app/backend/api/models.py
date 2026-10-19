from typing import Any
from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"
    version: str
    environment: str
    duckdb_connected: bool
    registered_views: list[str]


class ExecuteQueryRequest(BaseModel):
    sql: str = Field(..., description="Sentencia SQL de lectura a ejecutar")
    max_rows: int | None = Field(default=None, description="Límite máximo de registros a retornar")


class ExecuteQueryResponse(BaseModel):
    success: bool
    columns: list[str] = []
    data: list[dict[str, Any]] = []
    row_count: int = 0
    total_rows_matched: int = 0
    execution_time_ms: float = 0.0
    truncated: bool = False
    error: str | None = None


class ChatbotAskRequest(BaseModel):
    question: str = Field(..., description="Pregunta de negocio en lenguaje natural")
    dataset_hint: str | None = Field(default=None, description="Dataset sugerido (opcional)")


class ChatbotAskResponse(BaseModel):
    success: bool
    answer: str
    sql: str | None = None
    columns: list[str] = []
    data: list[dict[str, Any]] = []
    row_count: int = 0
    execution_time_ms: float = 0.0
    chart_suggestion: str | None = None

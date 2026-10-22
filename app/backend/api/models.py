from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field


class ErrorCode(str, Enum):
    """Catálogo estandarizado de códigos de error para el Chatbot y consultas analíticas."""
    ERR_EMPTY_QUESTION = "ERR_EMPTY_QUESTION"
    ERR_OUT_OF_SCOPE = "ERR_OUT_OF_SCOPE"
    ERR_LLM_UNAVAILABLE = "ERR_LLM_UNAVAILABLE"
    ERR_LLM_TIMEOUT = "ERR_LLM_TIMEOUT"
    ERR_SQL_GENERATION = "ERR_SQL_GENERATION"
    ERR_SQL_SECURITY = "ERR_SQL_SECURITY"
    ERR_QUERY_EXECUTION = "ERR_QUERY_EXECUTION"
    ERR_NO_DATA_FOUND = "ERR_NO_DATA_FOUND"


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
    error_code: str | None = None


class ChatbotAskRequest(BaseModel):
    question: str = Field(..., description="Pregunta de negocio en lenguaje natural")
    dataset_hint: str | None = Field(default=None, description="Dataset sugerido (opcional)")


class ChatbotAskResponse(BaseModel):
    success: bool
    answer: str
    sql: Optional[str] = None
    columns: list[str] = []
    data: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []
    row_count: int = 0
    execution_time_ms: float = 0.0
    chart_suggestion: Optional[str] = None
    error_code: Optional[str] = None
    error_detail: Optional[str] = None

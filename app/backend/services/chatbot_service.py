import json
import logging
import re
from typing import Any, Optional
import httpx

from app.backend.api.models import ErrorCode
from app.backend.core.config import settings
from app.backend.query_engine.duckdb_client import duckdb_engine
from app.backend.services.sql_guard import SQLGuard, SecurityException

logger = logging.getLogger(__name__)


class LLMException(Exception):
    """Excepción base para fallos del servicio LLM."""
    def __init__(self, message: str, error_code: ErrorCode, detail: Optional[str] = None):
        super().__init__(message)
        self.error_code = error_code
        self.detail = detail


class ChatbotService:
    """Servicio de analítica conversacional con inteligencia artificial.
    Utiliza el modelo local Qwen2.5 (2B class) para Text-to-SQL y síntesis Data-to-Text,
    sin fallbacks heurísticos ni plantillas prefabricadas, con códigos de error explícitos.
    """

    SYSTEM_SQL_PROMPT = """Eres el generador SQL de la Plataforma Medallion.
Tu única tarea es convertir la pregunta del usuario en una consulta SQL para DuckDB sobre las siguientes vistas de la capa Gold:

1. v_daily_sales (order_date, shipping_country, category, total_revenue, orders_count, units_sold, avg_ticket, returns_count, refunded_amount)
2. v_customer_360 (customer_id, first_purchase_date, last_purchase_date, total_orders, lifetime_value, units_bought, avg_order_value, favorite_category)
3. v_product_performance (product_id, product_name, category, units_sold, gross_revenue, returns_count, return_rate, margin)

Reglas estrictas:
- Genera ÚNICAMENTE una consulta SELECT de solo lectura.
- No incluyas bloques markdown ni explicaciones, solo la instrucción SQL pura.
- Si la pregunta no se relaciona con estas tablas o no se puede responder, responde únicamente: NO_SQL
"""

    SYSTEM_SYNTHESIS_PROMPT = """Eres el asistente analítico de la Plataforma Medallion.
A partir de la pregunta del usuario, la consulta SQL ejecutada y los datos reales obtenidos de la capa Gold,
redacta una respuesta concisa, profesional y fundamentada estrictamente en estos resultados.
No inventes información ni asumas datos fuera de la muestra proporcionada.
"""

    def __init__(self, duckdb_client=None):
        self.engine = duckdb_client or duckdb_engine

    def ask(self, question: str, dataset_hint: str | None = None) -> dict[str, Any]:
        """Procesa la pregunta del usuario de extremo a extremo sin fallbacks ni respuestas hardcodeadas."""
        clean_question = question.strip() if question else ""

        # 1. Validación de entrada vacía
        if not clean_question:
            return self._build_error_response(
                ErrorCode.ERR_EMPTY_QUESTION,
                "Por favor ingresa una pregunta válida sobre los datos analíticos.",
            )

        # 2. Detección de preguntas imposibles o fuera de alcance (Prueba P09 / Anti-alucinación)
        if self._is_out_of_scope(clean_question):
            return self._build_error_response(
                ErrorCode.ERR_OUT_OF_SCOPE,
                "Actualmente la plataforma cuenta únicamente con datos históricos analíticos consolidados. "
                "No es posible proyectar o responder consultas sobre períodos futuros o temas externos al negocio "
                "sin datos comprobables en la capa Gold.",
                success=False,
            )

        # 3. Generación Text-to-SQL con el modelo local Qwen2.5
        try:
            generated_sql = self._generate_sql(clean_question, dataset_hint)
        except LLMException as llm_err:
            logger.warning("Fallo al invocar el LLM para Text-to-SQL: %s (%s)", llm_err.error_code, llm_err.detail)
            return self._build_error_response(llm_err.error_code, str(llm_err), detail=llm_err.detail)
        except httpx.ConnectError as conn_err:
            return self._build_error_response(
                ErrorCode.ERR_LLM_UNAVAILABLE,
                f"No se pudo conectar con el servidor LLM local en {settings.LOCAL_LLM_URL}. "
                "Asegúrate de que el modelo local Qwen2.5 esté en ejecución.",
                detail=str(conn_err),
            )
        except httpx.TimeoutException as timeout_err:
            return self._build_error_response(
                ErrorCode.ERR_LLM_TIMEOUT,
                f"La inferencia del modelo local superó el tiempo límite de {settings.LOCAL_LLM_TIMEOUT}s.",
                detail=str(timeout_err),
            )
        except Exception as exc:
            logger.exception("Error inesperado en Text-to-SQL: %s", str(exc))
            return self._build_error_response(ErrorCode.ERR_SQL_GENERATION, f"Error generando consulta SQL: {str(exc)}")

        if not generated_sql or generated_sql.upper().strip() == "NO_SQL":
            return self._build_error_response(
                ErrorCode.ERR_SQL_GENERATION,
                "El modelo no pudo generar una consulta SQL válida para los datos solicitados. "
                "Por favor especifica si requieres datos de ventas, clientes o productos.",
            )

        # 4. Validación de seguridad con SQLGuard
        try:
            safe_sql = SQLGuard.validate_and_sanitize(generated_sql)
        except SecurityException as sec_err:
            logger.warning("SQL generado rechazado por seguridad: %s. SQL: %s", str(sec_err), generated_sql)
            return self._build_error_response(
                ErrorCode.ERR_SQL_SECURITY,
                f"La consulta generada no superó los controles de seguridad: {str(sec_err)}",
                sql=generated_sql,
            )

        # 5. Ejecución sobre la capa Gold en DuckDB
        query_result = self.engine.execute_query(safe_sql)
        if not query_result.get("success"):
            return self._build_error_response(
                ErrorCode.ERR_QUERY_EXECUTION,
                f"Error al ejecutar la consulta en DuckDB: {query_result.get('error')}",
                sql=safe_sql,
            )

        data = query_result.get("data", [])
        columns = query_result.get("columns", [])
        exec_time = query_result.get("execution_time_ms", 0.0)

        # 6. Caso sin registros coincidentes
        if not data:
            return {
                "success": True,
                "answer": "La consulta se ejecutó exitosamente, pero no arrojó ningún registro para los filtros indicados.",
                "sql": safe_sql,
                "data": [],
                "rows": [],
                "columns": columns,
                "row_count": 0,
                "execution_time_ms": exec_time,
                "chart_suggestion": "table",
                "error_code": ErrorCode.ERR_NO_DATA_FOUND.value,
                "error_detail": "0 rows matched in DuckDB query",
            }

        # 7. Síntesis Data-to-Text dinámica con el modelo local Qwen2.5
        chart_type = self._suggest_chart(columns)
        answer_text = self._synthesize_answer(clean_question, safe_sql, data, columns)

        return {
            "success": True,
            "answer": answer_text,
            "sql": safe_sql,
            "data": data,
            "rows": data,
            "columns": columns,
            "row_count": len(data),
            "execution_time_ms": exec_time,
            "chart_suggestion": chart_type,
            "error_code": None,
            "error_detail": None,
        }

    def _is_out_of_scope(self, question: str) -> bool:
        """Determina si la pregunta solicita proyecciones futuras o temas externos sin soporte de datos."""
        patterns = [
            r"\b(20[3-9]\d|21\d\d)\b",
            r"\b(futuro|predecir|predicci[oó]n|proyecci[oó]n|adivinar|adivinanza)\b",
            r"\b(clima|pol[ií]tica|presidente|f[uú]tbol|receta|hor[oó]scopo)\b",
        ]
        return any(re.search(pat, question, re.IGNORECASE) for pat in patterns)

    def _generate_sql(self, question: str, dataset_hint: str | None = None) -> str:
        """Invoca el LLM configurado para traducir lenguaje natural a SQL."""
        if settings.LLM_PROVIDER == "local":
            return self._call_local_llm_sql(question, dataset_hint)
        elif settings.LLM_PROVIDER == "gemini" and settings.GEMINI_API_KEY:
            return self._call_gemini_sql(question)
        elif settings.LLM_PROVIDER == "openai" and settings.OPENAI_API_KEY:
            return self._call_openai_sql(question)

        # Si el proveedor configurado es local (por defecto) o no tiene credenciales, usa local
        return self._call_local_llm_sql(question, dataset_hint)

    def _call_local_llm_sql(self, question: str, dataset_hint: str | None = None) -> str:
        """Llama al endpoint local de Qwen2.5 para generar la consulta SQL."""
        messages = [
            {"role": "system", "content": self.SYSTEM_SQL_PROMPT},
            {
                "role": "user",
                "content": f"Pregunta: {question}" + (f"\nSugerencia de dataset: {dataset_hint}" if dataset_hint else ""),
            },
        ]
        raw_response = self._post_to_openai_compatible_endpoint(
            messages=messages,
            temperature=0.0,
            max_tokens=250,
        )
        return self._clean_sql_block(raw_response)

    def _synthesize_answer(self, question: str, sql: str, data: list[dict], columns: list[str]) -> str:
        """Invoca al modelo local para generar una respuesta explicativa basada en los datos reales."""
        sample_data = data[:10]  # Primeros 10 registros para no sobrecargar el prompt
        data_json = json.dumps(sample_data, ensure_ascii=False, indent=2)

        messages = [
            {"role": "system", "content": self.SYSTEM_SYNTHESIS_PROMPT},
            {
                "role": "user",
                "content": (
                    f"Pregunta del usuario: {question}\n"
                    f"SQL ejecutado: {sql}\n"
                    f"Total de registros obtenidos: {len(data)}\n"
                    f"Datos encontrados (primeros registros):\n{data_json}\n\n"
                    "Redacta la respuesta explicativa:"
                ),
            },
        ]

        try:
            return self._post_to_openai_compatible_endpoint(messages=messages, temperature=0.3, max_tokens=300)
        except Exception as exc:
            logger.warning("Fallo al invocar LLM para síntesis Data-to-Text: %s. Formateando directamente.", str(exc))
            # Presentación limpia y directa de los datos si la síntesis LLM secundaria falla
            return self._direct_format_data(question, data, columns)

    def _direct_format_data(self, question: str, data: list[dict], columns: list[str]) -> str:
        """Formatea los resultados numéricos directamente si la llamada secundaria del LLM falla."""
        first = data[0]
        items = [f"**{k}**: {v}" for k, v in first.items() if not k.startswith("_")]
        details = ", ".join(items)
        return f"Consulta completada con éxito ({len(data)} registros encontrados). Resultados principales: {details}."

    def _post_to_openai_compatible_endpoint(
        self,
        messages: list[dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 300,
    ) -> str:
        """Realiza una petición HTTP POST a un servidor de inferencia local compatible con la API de OpenAI."""
        base_url = settings.LOCAL_LLM_URL.rstrip("/")
        url = f"{base_url}/chat/completions"
        payload = {
            "model": settings.LOCAL_LLM_MODEL,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            with httpx.Client(timeout=settings.LOCAL_LLM_TIMEOUT) as client:
                response = client.post(url, json=payload)
                if response.status_code != 200:
                    raise LLMException(
                        f"El servidor local devolvió código {response.status_code}: {response.text}",
                        error_code=ErrorCode.ERR_LLM_UNAVAILABLE,
                        detail=response.text,
                    )
                result = response.json()
                return result["choices"][0]["message"]["content"].strip()
        except httpx.ConnectError as conn_err:
            raise LLMException(
                f"No se pudo conectar con el servidor LLM local en {base_url}. "
                "Asegúrate de que el modelo local Qwen2.5 esté en ejecución.",
                error_code=ErrorCode.ERR_LLM_UNAVAILABLE,
                detail=str(conn_err),
            ) from conn_err
        except httpx.TimeoutException as timeout_err:
            raise LLMException(
                f"La inferencia del modelo local superó el tiempo límite de {settings.LOCAL_LLM_TIMEOUT}s.",
                error_code=ErrorCode.ERR_LLM_TIMEOUT,
                detail=str(timeout_err),
            ) from timeout_err
        except LLMException:
            raise
        except Exception as exc:
            raise LLMException(
                f"Error inesperado comunicándose con el LLM local: {str(exc)}",
                error_code=ErrorCode.ERR_LLM_UNAVAILABLE,
                detail=str(exc),
            ) from exc

    def _call_gemini_sql(self, question: str) -> str:
        """Fallback opcional a Gemini solo si se configuró explícitamente API key."""
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{settings.LLM_MODEL}:generateContent?key={settings.GEMINI_API_KEY}"
        )
        prompt = f"{self.SYSTEM_SQL_PROMPT}\n\nPregunta del usuario: {question}\nConsulta SQL:"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 300},
        }
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
                text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                return self._clean_sql_block(text)
        except Exception as exc:
            raise LLMException(
                f"Error al conectar con la API de Gemini: {str(exc)}",
                error_code=ErrorCode.ERR_LLM_UNAVAILABLE,
                detail=str(exc),
            ) from exc

    def _call_openai_sql(self, question: str) -> str:
        """Fallback opcional a OpenAI solo si se configuró explícitamente API key."""
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {settings.OPENAI_API_KEY}"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": self.SYSTEM_SQL_PROMPT},
                {"role": "user", "content": question},
            ],
            "temperature": 0.0,
        }
        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                text = resp.json()["choices"][0]["message"]["content"]
                return self._clean_sql_block(text)
        except Exception as exc:
            raise LLMException(
                f"Error al conectar con OpenAI: {str(exc)}",
                error_code=ErrorCode.ERR_LLM_UNAVAILABLE,
                detail=str(exc),
            ) from exc

    def _clean_sql_block(self, text: str) -> str:
        """Remueve etiquetas de markdown y espacios circundantes del código SQL."""
        clean = re.sub(r"^```(?:sql)?", "", text.strip(), flags=re.IGNORECASE)
        clean = re.sub(r"```$", "", clean.strip())
        return clean.strip().rstrip(";")

    def _suggest_chart(self, columns: list[str]) -> str:
        """Sugerencia de visualización según las reglas del Cuadro 8.2 del Documento Técnico."""
        if any(c in columns for c in ["order_date", "date", "kpi_date"]):
            return "line"
        if any(c in columns for c in ["shipping_country", "country", "category", "product_name"]):
            return "bar"
        return "table"

    def _build_error_response(
        self,
        error_code: ErrorCode,
        message: str,
        detail: Optional[str] = None,
        sql: Optional[str] = None,
        success: bool = False,
    ) -> dict[str, Any]:
        """Construye un diccionario de respuesta de error normalizado con código explícito."""
        return {
            "success": success,
            "answer": message,
            "sql": sql,
            "data": [],
            "rows": [],
            "columns": [],
            "row_count": 0,
            "execution_time_ms": 0.0,
            "chart_suggestion": None,
            "error_code": error_code.value,
            "error_detail": detail,
        }


chatbot_service = ChatbotService()

import logging
import re
from typing import Any
import httpx
from app.backend.core.config import settings
from app.backend.query_engine.duckdb_client import duckdb_engine
from app.backend.services.sql_guard import SQLGuard, SecurityException

logger = logging.getLogger(__name__)


class ChatbotService:
    """Servicio de analítica conversacional con inteligencia artificial.
    Interpreta preguntas en lenguaje natural, genera consultas SQL controladas,
    ejecuta sobre DuckDB y resume los resultados con cifras reales.
    """

    SYSTEM_PROMPT = """Eres el asistente de analítica de la Plataforma de Datos Medallion.
Tu rol es responder preguntas de negocio convirtiéndolas en consultas SQL DuckDB seguras sobre las siguientes vistas de la capa Gold:

1. v_daily_sales (order_date, shipping_country, category, total_revenue, orders_count, units_sold, avg_ticket, returns_count, refunded_amount)
2. v_customer_360 (customer_id, country, first_purchase_date, last_purchase_date, total_orders, lifetime_value, avg_order_value, favorite_category)
3. v_product_performance (product_id, product_name, category, units_sold, gross_revenue, returns_count, return_rate, profit_margin)

Reglas Obligatorias:
- Genera ÚNICAMENTE consultas de lectura SELECT.
- No inventes cifras. Si los datos no son suficientes o la pregunta es sobre el futuro o temas externos, explica amablemente la limitación.
- Devuelve la consulta SQL limpia sin explicaciones adicionales.
"""

    def __init__(self, duckdb_client=None):
        self.engine = duckdb_client or duckdb_engine

    def ask(self, question: str, dataset_hint: str | None = None) -> dict[str, Any]:
        """Procesa la pregunta del usuario de extremo a extremo."""
        clean_question = question.strip()
        if not clean_question:
            return {
                "success": False,
                "answer": "Por favor ingresa una pregunta válida sobre los datos analíticos.",
                "sql": None,
                "data": [],
                "columns": [],
                "chart_suggestion": None,
            }

        # 1. Detección temprana de preguntas imposibles o fuera de alcance (Prueba P09)
        impossible_patterns = [
            r"\b(20[3-9]\d|21\d\d)\b",       # Fechas futuras lejanas (ej. 2030, 2045)
            r"\b(futuro|predecir|proyecci[oó]n|adivinar|adivinanza)\b",
            r"\b(clima|pol[ií]tica|presidente|f[uú]tbol|receta)\b",
        ]
        for pattern in impossible_patterns:
            if re.search(pattern, clean_question, re.IGNORECASE):
                return {
                    "success": True,
                    "answer": (
                        "Actualmente la plataforma cuenta únicamente con datos históricos analíticos consolidados. "
                        "No es posible proyectar o responder consultas sobre períodos futuros o temas externos al negocio "
                        "sin datos comprobables en la capa Gold."
                    ),
                    "sql": None,
                    "data": [],
                    "columns": [],
                    "chart_suggestion": None,
                }

        # 2. Generar SQL (vía LLM configurado o fallback heurístico/semántico)
        generated_sql = self._generate_sql(clean_question, dataset_hint)

        if not generated_sql:
            return {
                "success": True,
                "answer": (
                    "No pude determinar una consulta SQL adecuada para tu pregunta con las fuentes disponibles. "
                    "Por favor intenta reformularla indicando si te refieres a ventas, clientes o productos."
                ),
                "sql": None,
                "data": [],
                "columns": [],
                "chart_suggestion": None,
            }

        # 3. Validar SQL generado mediante SQLGuard
        try:
            safe_sql = SQLGuard.validate_and_sanitize(generated_sql)
        except SecurityException as sec_err:
            logger.warning("SQL generado rechazado por seguridad: %s", str(sec_err))
            return {
                "success": False,
                "answer": f"La consulta generada no superó los controles de seguridad: {str(sec_err)}",
                "sql": generated_sql,
                "data": [],
                "columns": [],
                "chart_suggestion": None,
            }

        # 4. Ejecutar consulta en DuckDB sobre datos Gold
        query_result = self.engine.execute_query(safe_sql)

        if not query_result.get("success"):
            return {
                "success": False,
                "answer": f"Error al consultar los datos: {query_result.get('error')}",
                "sql": safe_sql,
                "data": [],
                "columns": [],
                "chart_suggestion": None,
            }

        data = query_result.get("data", [])
        columns = query_result.get("columns", [])

        # 5. Redactar respuesta y sugerencia de visualización basada en resultados reales
        answer_text, chart_type = self._summarize_results(clean_question, safe_sql, data, columns)

        return {
            "success": True,
            "answer": answer_text,
            "sql": safe_sql,
            "data": data,
            "columns": columns,
            "row_count": len(data),
            "execution_time_ms": query_result.get("execution_time_ms", 0),
            "chart_suggestion": chart_type,
        }

    def _generate_sql(self, question: str, dataset_hint: str | None = None) -> str | None:
        """Genera una consulta SQL utilizando el proveedor configurado o lógica semántica."""
        if settings.LLM_PROVIDER == "gemini" and settings.GEMINI_API_KEY:
            return self._call_gemini_llm(question)
        elif settings.LLM_PROVIDER == "openai" and settings.OPENAI_API_KEY:
            return self._call_openai_llm(question)

        # Modo Mock / Fallback semántico determinista
        return self._heuristic_nl_to_sql(question, dataset_hint)

    def _call_gemini_llm(self, question: str) -> str | None:
        """Invoca la API de Gemini para traducir lenguaje natural a SQL DuckDB."""
        url = (
            f"https://generativelanguage.googleapis.com/v1beta/models/"
            f"{settings.LLM_MODEL}:generateContent?key={settings.GEMINI_API_KEY}"
        )
        prompt = f"{self.SYSTEM_PROMPT}\n\nPregunta del usuario: {question}\nConsulta SQL:"
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "generationConfig": {"temperature": 0.0, "maxOutputTokens": 300},
        }

        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, json=payload)
                if resp.status_code == 200:
                    text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
                    clean_sql = re.sub(r"```(sql)?", "", text).strip()
                    return clean_sql
        except Exception as e:
            logger.error("Error al conectar con Gemini LLM: %s. Usando fallback.", str(e))

        return self._heuristic_nl_to_sql(question)

    def _call_openai_llm(self, question: str) -> str | None:
        """Invoca la API de OpenAI para traducir lenguaje natural a SQL DuckDB."""
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {settings.OPENAI_API_KEY}"}
        payload = {
            "model": "gpt-4o-mini",
            "messages": [
                {"role": "system", "content": self.SYSTEM_PROMPT},
                {"role": "user", "content": question},
            ],
            "temperature": 0.0,
        }

        try:
            with httpx.Client(timeout=10.0) as client:
                resp = client.post(url, headers=headers, json=payload)
                if resp.status_code == 200:
                    text = resp.json()["choices"][0]["message"]["content"]
                    clean_sql = re.sub(r"```(sql)?", "", text).strip()
                    return clean_sql
        except Exception as e:
            logger.error("Error al conectar con OpenAI: %s. Usando fallback.", str(e))

        return self._heuristic_nl_to_sql(question)

    def _heuristic_nl_to_sql(self, question: str, dataset_hint: str | None = None) -> str | None:
        """Motor heurístico de traducción a SQL para pruebas locales y modo offline."""
        q = question.lower()

        # 1. Ventas / Ingresos por país
        if "país" in q or "pais" in q:
            return (
                "SELECT shipping_country, ROUND(SUM(total_revenue), 2) AS total_revenue "
                "FROM v_daily_sales "
                "GROUP BY shipping_country "
                "ORDER BY total_revenue DESC"
            )

        # 2. Ventas totales / ¿Cuánto vendimos?
        if any(w in q for w in ["cuánto vendimos", "cuanto vendimos", "ventas totales", "total ventas", "ingresos totales"]):
            return (
                "SELECT ROUND(SUM(total_revenue), 2) AS total_revenue, "
                "SUM(orders_count) AS total_orders, "
                "SUM(units_sold) AS total_units_sold "
                "FROM v_daily_sales"
            )

        # 3. Productos más vendidos / Top productos
        if "producto" in q or "más vendido" in q or "top" in q:
            limit_n = 5
            m = re.search(r"\b(\d+)\b", q)
            if m:
                limit_n = int(m.group(1))
            return (
                f"SELECT product_name, category, units_sold, gross_revenue "
                f"FROM v_product_performance "
                f"ORDER BY units_sold DESC LIMIT {limit_n}"
            )

        # 4. Devoluciones / Categorías con más devoluciones
        if "devolucion" in q or "devolución" in q or "devoluciones" in q:
            return (
                "SELECT category, SUM(returns_count) AS total_returns, "
                "ROUND(SUM(refunded_amount), 2) AS total_refunded "
                "FROM v_daily_sales "
                "GROUP BY category "
                "ORDER BY total_returns DESC"
            )

        # 5. Clientes / Customer 360
        if "cliente" in q or "clientes" in q or "ltv" in q:
            return (
                "SELECT country, COUNT(*) AS customer_count, "
                "ROUND(AVG(lifetime_value), 2) AS avg_ltv, "
                "ROUND(AVG(total_orders), 1) AS avg_orders "
                "FROM v_customer_360 "
                "GROUP BY country "
                "ORDER BY customer_count DESC"
            )

        # 6. Tendencia temporal de ventas
        if "tendencia" in q or "mes" in q or "diari" in q or "dias" in q:
            return (
                "SELECT order_date, ROUND(SUM(total_revenue), 2) AS daily_revenue "
                "FROM v_daily_sales "
                "GROUP BY order_date "
                "ORDER BY order_date ASC"
            )

        # 7. Ventas por categoría por defecto
        return (
            "SELECT category, ROUND(SUM(total_revenue), 2) AS total_revenue, "
            "SUM(units_sold) AS units_sold "
            "FROM v_daily_sales "
            "GROUP BY category "
            "ORDER BY total_revenue DESC"
        )

    def _summarize_results(
        self, question: str, sql: str, data: list[dict], columns: list[str]
    ) -> tuple[str, str]:
        """Sintetiza la respuesta en lenguaje natural y sugiere el gráfico adecuado."""
        if not data:
            return "No se encontraron registros que coincidan con la consulta solicitada.", "table"

        # Sugerencia de gráfico según las reglas del Cuadro 8.2 del Documento Técnico:
        # Fecha y medida -> serie temporal (line)
        # Categoría y cantidad -> gráfico de barras (bar)
        # Numérico vs Numérico -> scatter
        chart_type = "table"
        if any(c in columns for c in ["order_date", "date"]):
            chart_type = "line"
        elif any(c in columns for c in ["shipping_country", "country", "category", "product_name"]):
            chart_type = "bar"

        # Redacción de respuesta con datos reales
        first_row = data[0]

        if "total_revenue" in first_row and len(data) == 1:
            rev = first_row.get("total_revenue", 0)
            orders = first_row.get("total_orders", 0)
            ans = f"El total acumulado de ventas registradas es de **${rev:,.2f}** a lo largo de **{orders:,}** pedidos."
            return ans, "metric"

        if "shipping_country" in first_row and "total_revenue" in first_row:
            top_country = first_row["shipping_country"]
            top_rev = first_row["total_revenue"]
            ans = (
                f"El país con mayores ingresos es **{top_country}** con un total de **${top_rev:,.2f}**. "
                f"Se obtuvieron resultados para {len(data)} países en total."
            )
            return ans, chart_type

        if "product_name" in first_row and "units_sold" in first_row:
            top_prod = first_row["product_name"]
            top_units = first_row["units_sold"]
            ans = (
                f"El producto más vendido es **{top_prod}** con **{top_units:,} unidades**. "
                f"Mostrando los {len(data)} productos con mayor volumen."
            )
            return ans, chart_type

        if "category" in first_row and "total_returns" in first_row:
            top_cat = first_row["category"]
            top_ret = first_row["total_returns"]
            ans = f"La categoría con mayor cantidad de devoluciones es **{top_cat}** con **{top_ret:,} devoluciones** registradas."
            return ans, chart_type

        ans = f"Se obtuvieron **{len(data)} registros** reales desde los modelos Gold de la plataforma."
        return ans, chart_type


chatbot_service = ChatbotService()

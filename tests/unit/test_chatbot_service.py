from unittest.mock import patch
import httpx
import pytest

from app.backend.api.models import ErrorCode
from app.backend.services.chatbot_service import ChatbotService


@pytest.fixture
def service():
    return ChatbotService()


def test_chatbot_empty_question(service):
    res = service.ask("   ")
    assert res["success"] is False
    assert res["error_code"] == ErrorCode.ERR_EMPTY_QUESTION.value
    assert "pregunta válida" in res["answer"]


def test_chatbot_impossible_question_p09(service):
    # Prueba P09: Pregunta sobre el futuro lejano o imposible
    res = service.ask("¿Cuál será la proyección de ventas para el año 2045?")
    assert res["success"] is False
    assert res["error_code"] == ErrorCode.ERR_OUT_OF_SCOPE.value
    assert res["sql"] is None
    assert "históricos" in res["answer"].lower()
    assert len(res["data"]) == 0


def test_chatbot_llm_connection_error(service):
    # Simula falla de conexión al endpoint local (ej. Ollama apagado)
    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        side_effect=httpx.ConnectError("Connection refused to localhost:11434"),
    ):
        res = service.ask("¿Cuánto vendimos en total?")
        assert res["success"] is False
        assert res["error_code"] == ErrorCode.ERR_LLM_UNAVAILABLE.value
        assert "No se pudo conectar" in res["answer"]


def test_chatbot_llm_timeout_error(service):
    # Simula timeout de inferencia en el modelo local
    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        side_effect=httpx.TimeoutException("Read timed out"),
    ):
        res = service.ask("¿Cuánto vendimos en total?")
        assert res["success"] is False
        assert res["error_code"] == ErrorCode.ERR_LLM_TIMEOUT.value
        assert "tiempo límite" in res["answer"]


def test_chatbot_sql_security_violation(service):
    # Simula que el modelo intentó generar un DROP TABLE
    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        return_value="DROP TABLE v_daily_sales;",
    ):
        res = service.ask("Elimina la tabla de ventas")
        assert res["success"] is False
        assert res["error_code"] == ErrorCode.ERR_SQL_SECURITY.value
        assert "controles de seguridad" in res["answer"]


def test_chatbot_invalid_sql_generated(service):
    # Simula que el modelo devolvió texto plano o NO_SQL
    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        return_value="NO_SQL",
    ):
        res = service.ask("¿Cómo se cocina una pasta?")
        assert res["success"] is False
        assert res["error_code"] == ErrorCode.ERR_SQL_GENERATION.value


def test_chatbot_no_data_found(service):
    # Simula consulta que no arroja filas en DuckDB
    sql_mock = "SELECT * FROM v_daily_sales WHERE shipping_country = 'PAIS_INEXISTENTE'"
    with patch.object(service, "_post_to_openai_compatible_endpoint", return_value=sql_mock):
        res = service.ask("¿Cuánto vendimos en un país inventado?")
        assert res["success"] is True
        assert res["error_code"] == ErrorCode.ERR_NO_DATA_FOUND.value
        assert res["row_count"] == 0
        assert len(res["data"]) == 0


def test_chatbot_successful_flow(service):
    # Simula flujo exitoso: 1er llamado genera SQL, 2do llamado sintetiza respuesta
    sql_mock = "SELECT shipping_country, SUM(total_revenue) AS total_revenue FROM v_daily_sales GROUP BY shipping_country ORDER BY total_revenue DESC LIMIT 5"
    synthesis_mock = "Los 5 países con mayores ventas registradas son encabezados por los resultados mostrados en la tabla adjunta."

    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        side_effect=[sql_mock, synthesis_mock],
    ):
        res = service.ask("¿Cuáles son los 5 países con más ventas?")
        assert res["success"] is True
        assert res["error_code"] is None
        assert res["sql"] is not None
        assert res["row_count"] > 0
        assert len(res["data"]) > 0
        assert len(res["rows"]) > 0
        assert res["answer"] == synthesis_mock
        assert res["chart_suggestion"] in ("bar", "line", "table")


def test_chatbot_date_sub_normalization(service):
    # Simula que el LLM genera DATE_SUB(CURRENT_DATE, 1) como en el caso reportado por el usuario
    sql_with_date_sub = "SELECT total_revenue FROM v_daily_sales WHERE order_date = DATE_SUB(CURRENT_DATE, 1) LIMIT 500"
    synthesis_mock = "El ingreso de ayer fue registrado exitosamente."

    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        side_effect=[sql_with_date_sub, synthesis_mock],
    ):
        res = service.ask("Cual es el ingreso total de ayer")
        assert res["success"] is True
        assert res["error_code"] is None
        # Verifica que el SQL ejecutado fue normalizado para DuckDB y no tiene DATE_SUB
        assert "DATE_SUB" not in res["sql"].upper()
        assert "INTERVAL" in res["sql"].upper()


def test_chatbot_conversational_project_question(service):
    # Prueba que preguntas como 'Como funciona el proyecto' se respondan conversacionalmente sin fallar
    explanation = "La plataforma opera bajo una arquitectura Medallion (Landing -> Bronze -> Silver -> Gold + Quarantine) procesando datos con MinIO, Spark y DuckDB."

    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        return_value=explanation,
    ):
        res = service.ask("Como funciona el proyecto")
        assert res["success"] is True
        assert res["error_code"] is None
        assert res["sql"] is None
        assert len(res["data"]) == 0
        assert "Medallion" in res["answer"]


def test_chatbot_bronze_layer_query(service):
    # Prueba consulta sobre la capa bronze (ejecutada sobre v_pipeline_execution_logs)
    sql_bronze = "SELECT records_processed FROM v_pipeline_execution_logs WHERE UPPER(layer) = 'BRONZE' LIMIT 1"
    synthesis_mock = "En la capa Bronze se procesaron 5,000 registros exitosamente."

    with patch.object(
        service,
        "_post_to_openai_compatible_endpoint",
        side_effect=[sql_bronze, synthesis_mock],
    ):
        res = service.ask("cuantos datos hay en capa bronze")
        assert res["success"] is True
        assert res["error_code"] is None
        assert res["sql"] is not None
        assert res["row_count"] > 0


def test_chatbot_cuantas_capas_manejas(service):
    # Prueba específica para la pregunta reportada por el usuario en la captura de pantalla
    res = service.ask("cuantas capas manejas?")
    assert res["success"] is True
    assert res["error_code"] is None
    assert res["sql"] is None
    assert "Medallion" in res["answer"]
    assert "Bronze" in res["answer"]
    assert "Silver" in res["answer"]
    assert "Gold" in res["answer"]


def test_chatbot_direct_data_formatting(service):
    # Verifica que el formateo numérico directo genera formato moneda y porcentajes correctos
    data = [{"total_revenue": 9860.19, "return_rate": 0.05}]
    formatted = service._direct_format_data("Cuál fue el ingreso de ayer?", data, ["total_revenue", "return_rate"])
    assert "$9,860.19" in formatted
    assert "5.00%" in formatted
    assert "Gold" in formatted

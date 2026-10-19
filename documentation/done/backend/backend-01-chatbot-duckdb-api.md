# Entregable: Backend, Capa de Consulta DuckDB y Chatbot IA (US-12 y US-13)

> **Responsable:** Dylan Gamero (Líder y Chatbot IA)  
> **Rama:** `feat/backend-chatbot-ai`  
> **Fecha de Finalización:** 19 de octubre de 2026  
> **Estado:** Completo y verificado (27/27 tests aprobados)

---

## 1. Resumen de Historias de Usuario Implementadas

- **US-12: Capa de Consulta SQL sobre Parquet con DuckDB y API Backend en FastAPI**
  - Implementación del motor `DuckDBClient` que monta vistas SQL sobre la capa Gold (`v_daily_sales`, `v_customer_360`, `v_product_performance`).
  - Endpoints analíticos estandarizados para consumo de dashboards y visualizadores.
  - Tiempos de respuesta inferiores a 10 ms sobre archivos Parquet locales.
- **US-13: Asistente Conversacional (Chatbot IA) con Text-to-SQL Seguro y Explicabilidad**
  - Servicio `ChatbotService` capaz de traducir lenguaje natural a SQL DuckDB con proveedor configurable (Gemini, OpenAI o motor semántico local).
  - Validador estricto de seguridad `SQLGuard` con bloqueo de sentencias DDL/DML (`DROP`, `DELETE`, `INSERT`, etc.), detección de inyecciones y forzado de cláusula `LIMIT`.
  - Mecanismo anti-alucinación verificado (Prueba P09): si la pregunta es sobre el futuro lejano o imposible, el sistema responde explicando la limitación sin inventar cifras.
  - Respuestas explicativas acompañadas de sugerencias de visualización gráfica (`line`, `bar`, `table`, `metric`) basadas en el Cuadro 8.2 del Documento Técnico.

---

## 2. Estructura de Componentes Generados

```text
pipeline-analitica/
|-- app/
|   `-- backend/
|       |-- api/
|       |   |-- models.py           # DTOs y schemas Pydantic
|       |   `-- routes.py           # Endpoints REST (/health, /metrics, /query, /chatbot)
|       |-- core/
|       |   `-- config.py           # Settings Pydantic con variables de entorno
|       |-- query_engine/
|       |   `-- duckdb_client.py    # Motor DuckDB con registro de vistas Parquet
|       |-- services/
|       |   |-- chatbot_service.py  # Orquestador NL-to-SQL y generador de explicaciones
|       |   `-- sql_guard.py        # Validador de seguridad y guardrails SQL
|       `-- main.py                 # Servidor FastAPI con CORS y ciclo de vida
|-- data/
|   `-- mock/
|       `-- gold/                   # Muestras Parquet generadas (daily_sales, customer_360, product_performance)
|-- scripts/
|   `-- generate_gold_mock.py       # Script generador de Parquet para capa Gold
|-- tests/
|   |-- unit/
|   |   |-- test_sql_guard.py       # 10 tests unitarios de seguridad SQL
|   |   |-- test_duckdb_client.py   # 5 tests unitarios del cliente DuckDB
|   |   `-- test_chatbot_service.py # 4 tests del Chatbot y anti-alucinación
|   `-- integration/
|       `-- test_api_endpoints.py   # 8 tests de integración con TestClient
|-- pytest.ini                      # Configuración de pruebas
|-- requirements.txt                # Dependencias del backend
|-- .env.example                    # Plantilla de variables de entorno
`-- .gitignore                      # Reglas de exclusión de git
```

---

## 3. Catálogo de Endpoints REST

| Método | Endpoint | Descripción | Parámetros / Payload |
| :--- | :--- | :--- | :--- |
| `GET` | `/` | Información general de la API | - |
| `GET` | `/api/v1/health` | Estado del servicio y vistas registradas | - |
| `GET` | `/api/v1/catalog/schemas` | Esquemas y columnas de las tablas Gold | - |
| `GET` | `/api/v1/metrics/daily-sales` | KPIs y serie temporal agregada | `start_date`, `end_date`, `country`, `category` |
| `GET` | `/api/v1/metrics/top-products` | Ranking de productos por volumen/ingresos | `limit`, `category` |
| `GET` | `/api/v1/metrics/customer-summary`| Resumen métrico Customer 360 | - |
| `POST` | `/api/v1/query/execute` | Ejecución de consulta SQL validada | `{"sql": "...", "max_rows": 100}` |
| `POST` | `/api/v1/chatbot/ask` | Consulta conversacional al Chatbot | `{"question": "...", "dataset_hint": "..."}` |

---

## 4. Resultados de Pruebas de Funcionamiento

Ejecución de la suite completa con `pytest tests/`:
- **Total de pruebas:** 27
- **Aprobadas:** 27
- **Fallidas:** 0
- **Tiempo de ejecución:** 1.64s

```text
tests/integration/test_api_endpoints.py ........                         [ 29%]
tests/unit/test_chatbot_service.py ....                                  [ 44%]
tests/unit/test_duckdb_client.py .....                                   [ 62%]
tests/unit/test_sql_guard.py ..........                                  [100%]
======================== 27 passed, 1 warning in 1.64s =========================
```

# Documentación del Proyecto: Pipeline Analítica

Bienvenido a la carpeta de documentación del proyecto **Plataforma Extensible de Datos (Pipeline Analítica)**.

## Documentos Principales

- [**Documento Técnico: Arquitectura Medallion**](file:///home/dypok/Projects/pipeline-analitica/documentation/arquitectura-medallion.md)
  - Especificación completa de la plataforma de datos versión 2.0 (octubre de 2026).
  - Alcance, requerimientos funcionales, arquitectura de capas Medallion (Landing, Bronze, Silver, Gold, Quarantine).
  - Modelos analíticos (`daily_sales`, `customer_360`, `product_performance`).
  - Capa de consulta (DuckDB), dashboards (Streamlit / Plotly), chatbot analítico con IA (FastAPI / SQL seguro).
  - Orquestación con Apache Airflow, linaje con OpenMetadata y gobernanza con Apache Atlas.
  - Distribución del equipo de trabajo, plan de implementación por fases y matrices de riesgos y pruebas.

## Estructura de Documentación

```text
documentation/
|-- README.md                     <-- Índice de la documentación
|-- arquitectura-medallion.md     <-- Documento técnico y arquitectura Medallion v2.0
`-- changelog/                    <-- Registro diario de cambios
    `-- DD-MM-YYYY-changelog.md
```

---
marp: true
theme: default
paginate: true
size: 16:9
style: |
  section {
    background-color: #0f172a;
    color: #f8fafc;
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    padding: 40px 60px;
  }
  h1 {
    color: #38bdf8;
    font-size: 2.2rem;
    margin-bottom: 0.3rem;
  }
  h2 {
    color: #94a3b8;
    font-size: 1.4rem;
    font-weight: 500;
  }
  h3 {
    color: #e2e8f0;
    font-size: 1.15rem;
  }
  .highlight {
    color: #38bdf8;
    font-weight: 600;
  }
  .card-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-top: 20px;
  }
  .card {
    background: #1e293b;
    border: 1px solid #334155;
    border-radius: 12px;
    padding: 16px;
  }
  .badge {
    display: inline-block;
    padding: 4px 10px;
    border-radius: 9999px;
    font-size: 0.75rem;
    font-weight: 600;
    text-transform: uppercase;
  }
  footer {
    color: #64748b;
    font-size: 0.8rem;
  }
---

<!-- Slide 1: Portada -->
# Plataforma Extensible de Datos
## Arquitectura Medallion, Gobernanza Activa y Analítica Asistida por IA Local

---
**Versión:** 2.0 • **Formato:** 16:9 • **Fecha:** Octubre 2026  
**Enfoque:** Lakehouse Modular, DuckDB, Streamlit y LLM Soberano (Qwen 2.5)

*Notas del Orador:*
*Buenos días a todos. Hoy presentamos la Plataforma Extensible de Datos, una solución de ingeniería de datos integral diseñada para capturar, procesar, certificar y consultar información empresarial a escala sin fricciones, integrando dashboards visuales y consultas en lenguaje natural con inteligencia artificial soberana y sin costes de API.*

---

<!-- Slide 2: El Problema de Negocio y Técnico -->
# El Problema y Desafíos Actuales
## ¿Por qué las organizaciones no confían en sus datos ni aprovechan la IA?

<div class="card-grid">
  <div class="card">
    <h3 style="color: #f87171;">⚠️ Silos y Formatos Heterogéneos</h3>
    <p style="font-size: 0.9rem; color: #cbd5e1;">Datos dispersos en archivos CSV estáticos, payloads JSON no normalizados y APIs externas sin contrato común.</p>
  </div>
  <div class="card">
    <h3 style="color: #fbbf24;">🔍 Linaje Opaco y Datos Corruptos</h3>
    <p style="font-size: 0.9rem; color: #cbd5e1;">Falta de trazabilidad: cuando una métrica falla, es imposible saber qué transformación o fuente la alteró.</p>
  </div>
  <div class="card">
    <h3 style="color: #a78bfa;">🤖 Alucinación y Riesgo en IA</h3>
    <p style="font-size: 0.9rem; color: #cbd5e1;">Los chatbots tradicionales inventan números o dependen de costosas APIs externas con riesgo de fuga de datos sensibles.</p>
  </div>
</div>

*Notas del Orador:*
*Las empresas enfrentan tres grandes dolores: datos desarticulados, falta de confianza en los números por carecer de linaje y calidad, y la adopción deficiente de IA por alucinaciones y altos costos en la nube.*

---

<!-- Slide 3: La Propuesta de Valor -->
# Propuesta de Valor y Principios de Diseño
## Consistencia garantizada: Una única fuente de verdad (Single Source of Truth)

- **Arquitectura Lakehouse Medallion:** Flujo desacoplado en capas de madurez (*Landing, Bronze, Silver, Gold, Quarantine*).
- **Procesamiento Distribuido e Idempotente:** Apache Spark garantizando reprocesamiento seguro sin duplicaciones.
- **Doble Canal de Consumo Unificado:**
  - **Dashboards Visuales (Streamlit):** Monitoreo analítico de KPIs ejecutivos en tiempo real.
  - **Chatbot Analítico Seguro:** Consultas en lenguaje natural con traducción estricta a SQL mediante LLM local.
- **Soberanía y Costo Cero de Inferencia:** Modelo Qwen 2.5 (2B) ejecutándose en infraestructura propia.

*Notas del Orador:*
*Nuestra premisa fundamental es que el dashboard y el chatbot consumen exactamente los mismos datos y las mismas fórmulas. El chatbot nunca inventa números: traduce preguntas a consultas SQL seguras que se ejecutan directamente sobre los datos certificados.*

---

<!-- Slide 4: Arquitectura General de la Solución -->
# Arquitectura End-to-End
## De la ingesta heterogénea al valor analítico accionable

```text
[ FUENTES ] ───────► [ INGESTA & LAKEHOUSE (MinIO) ] ───────► [ PROCESAMIENTO ]
 - APIs REST           - Landing: Crudo original                - Apache Spark
 - Archivos CSV        - Bronze: Tipado e Ingestado             - Calidad & Quarantine
 - Payloads JSON       - Silver: Limpio & Enriquecido
                       - Gold: Modelos Analíticos
                                     │
                                     ▼
[ GOBERNANZA & OPS ]  ◄────── [ MOTOR DUCKDB ] ──────►  [ CAPA DE CONSUMO ]
 - Airflow (DAGs)              Baja latencia             - Dashboards Streamlit
 - OpenMetadata (Linaje)       Consultas Parquet         - Chatbot IA (FastAPI +
 - Apache Atlas (Catálogo)                                 Qwen 2.5 Local)
```

*Notas del Orador:*
*Aquí observamos el flujo completo de la plataforma. La ingesta alimenta el almacenamiento en MinIO; Spark procesa y clasifica lo válido a Silver y lo erróneo a Quarantine; Gold almacena Data Marts en formato Parquet, que son consumidos a velocidad sub-segundo por DuckDB tanto para Streamlit como para el Chatbot.*

---

<!-- Slide 5: Detalle de Capas Medallion -->
# Arquitectura Medallion al Detalle
## Trazabilidad y elevación progresiva de la calidad del dato

| Capa | Almacenamiento | Propósito y Criterios |
| :--- | :--- | :--- |
| **Landing** | MinIO (Raw) | Recepción de payloads crudos inmutables con metadatos (`ingestion_timestamp`, `source_id`). |
| **Bronze** | MinIO / Parquet | Datos históricos estructurados, esquemas validados y preservación íntegra de registros. |
| **Silver** | MinIO / Spark | Deduplicación, normalización, conversión de tipos y validación de reglas de negocio. |
| **Quarantine** | MinIO / Logs | Registros anómalos o corruptos aislados para auditoría sin romper la ejecución del pipeline. |
| **Gold** | Parquet optimizado | Tablas dimensionales y agregaciones especializadas listas para consumo analítico. |

*Notas del Orador:*
*Cada capa tiene un rol claro. Una característica clave es la zona de Cuarentena: si un registro viene con valores negativos o fechas inválidas, no aborta el lote completo; se aísla en cuarentena con el motivo del fallo para su posterior revisión.*

---

<!-- Slide 6: Modelos Analíticos en Capa Gold -->
# Capa Gold: Modelos de Negocio
## Estructuras optimizadas para respuesta inmediata

<div class="card-grid">
  <div class="card">
    <h3 style="color: #38bdf8;">📊 daily_sales</h3>
    <p style="font-size: 0.85rem; color: #cbd5e1;">Métricas de ingresos diarios, volumen de pedidos completados, tickets promedio y descuentos aplicados agregados por fecha y canal.</p>
  </div>
  <div class="card">
    <h3 style="color: #34d399;">👥 customer_360</h3>
    <p style="font-size: 0.85rem; color: #cbd5e1;">Vista integral del cliente: Recencia, Frecuencia y Valor Monetario (RFM), valor de vida (LTV) y categorización de comportamiento.</p>
  </div>
  <div class="card">
    <h3 style="color: #fbbf24;">📦 product_performance</h3>
    <p style="font-size: 0.85rem; color: #cbd5e1;">Rotación de stock, márgenes de utilidad bruta, ranking de productos más vendidos y tasa de devolución.</p>
  </div>
</div>

*Notas del Orador:*
*Estos tres modelos resuelven más del 90% de las preguntas frecuentes de negocio: cuánto vendemos, quién nos compra y qué productos generan mayor rentabilidad.*

---

<!-- Slide 7: Motor de Consulta DuckDB & Dashboards -->
# Visualización e Hiper-Rendimiento
## DuckDB + Streamlit: Analítica interactiva en milisegundos

- **DuckDB como Motor Analítico Central:**
  - Motor SQL columnar embebido de procesamiento vectorial.
  - Lee directamente sobre archivos Parquet en MinIO / almacenamiento local sin necesidad de bases de datos pesadas intermedias.
  - Tiempos de respuesta sub-segundo para agregaciones complejas.
- **Dashboards Interactivos en Streamlit:**
  - Vistas ejecutivas y operativas con Plotly.
  - Filtros reactivos por fechas, categorías y clientes.
  - Control de accesos y consumo directo desde la capa Gold.

*Notas del Orador:*
*Al usar DuckDB directamente sobre los archivos Parquet de la capa Gold, eliminamos la necesidad de mantener costosos data warehouses en la nube para el consumo inicial, logrando latencias de consulta inferiores a 200 ms.*

---

<!-- Slide 8: Chatbot con IA Local y Cero Fallbacks -->
# Chatbot Analítico Inteligente
## NL-to-SQL Soberano: Modelo Qwen 2.5 (2B) sin dependencias externas

```text
[ Pregunta Usuario ] ──► [ Prompt Seguro ] ──► [ Qwen 2.5 Local (Ollama) ]
                                                       │ (Genera SQL)
                                                       ▼
[ Respuesta Sintetizada ] ◄── [ DuckDB ] ◄── [ Validador SQLGuard ]
     (Data-to-Text)           (Ejecuta)         - Solo SELECT permitido
                                                - Rechaza mutaciones / DDL
```

- **100% Local:** Modelo de 2B parámetros ejecutado vía Ollama / vLLM. Sin costes por token ni fugas de privacidad.
- **Eliminación Total de Fallbacks:** Respuestas auténticas fundamentadas en datos reales.
- **Catálogo de Códigos de Error (`ErrorCode`):** `ERR_EMPTY_QUESTION`, `ERR_OUT_OF_SCOPE`, `ERR_LLM_UNAVAILABLE`, `ERR_SQL_SECURITY`, `ERR_NO_DATA_FOUND`.

*Notas del Orador:*
*El usuario pregunta en español cotidiano. El modelo local Qwen2.5 genera la consulta SQL, SQLGuard verifica que sea segura y de solo lectura, DuckDB la ejecuta y el modelo sintetiza la respuesta. Si algo falla o no hay datos, se entrega un código de error preciso y transparente.*

---

<!-- Slide 9: Orquestación, Linaje y Gobernanza -->
# Gobernanza, Linaje y Operación Continua
## Control total de extremo a extremo

<div class="card-grid">
  <div class="card">
    <h3 style="color: #60a5fa;">⏱️ Apache Airflow</h3>
    <p style="font-size: 0.85rem; color: #cbd5e1;">Orquestación basada en DAGs con ejecución programada, idempotencia, reintentos automáticos y registro de métricas operativas.</p>
  </div>
  <div class="card">
    <h3 style="color: #a78bfa;">🗺️ OpenMetadata</h3>
    <p style="font-size: 0.85rem; color: #cbd5e1;">Mapeo visual del linaje de datos de extremo a extremo: desde la API de origen hasta la tarjeta del dashboard o respuesta del bot.</p>
  </div>
  <div class="card">
    <h3 style="color: #f472b6;">🛡️ Apache Atlas</h3>
    <p style="font-size: 0.85rem; color: #cbd5e1;">Catálogo de gobernanza empresarial, clasificación de datos personales (PII) y trazabilidad de políticas de acceso.</p>
  </div>
</div>

*Notas del Orador:*
*La gobernanza no es un añadido opcional; está integrada en el diseño. Podemos saber con exactitud de qué tabla y de qué archivo se originó cada cálculo que ve el usuario final.*

---

<!-- Slide 10: Estrategia de Entrega y Calidad de Software -->
# Entrega Técnica y Pruebas Rigurosas
## Desarrollo paralelo desacoplado con datos sintéticos y 82 pruebas automáticas

- **Metodología Mock-First:**
  - Generación de datos sintéticos controlados para habilitar el desarrollo paralelo de los 7 roles sin cuellos de botella de integración.
- **Matriz de Pruebas P01 a P12:**
  - Validación de contratos de esquema, idempotencia, aislamiento de cuarentena y seguridad SQL.
- **Suite Automatizada de Pruebas:**
  - **82 pruebas unitarias e integración** ejecutadas en menos de 4 segundos con Pytest.
  - Verificación continua sin sobrecargar el hardware de desarrollo.

*Notas del Orador:*
*Para evitar que el equipo de visualización esperara a que el equipo de ingesta terminara, implementamos generadores de datos sintéticos consistentes. Además, la suite de 82 pruebas garantiza que cada pull request llegue verificado y seguro a producción.*

---

<!-- Slide 11: Resumen de Beneficios e Impacto -->
# Impacto y Ventajas Competitivas
## Una solución moderna, eficiente y financieramente inteligente

| Dimensión | Enfoque Tradicional | Nuestra Plataforma |
| :--- | :--- | :--- |
| **Costos de IA** | Facturación recurrente por token en la nube | **\$0 en inferencia** con LLM 2B local |
| **Confiabilidad** | Métricas discordantes y alucinaciones | **Mismo motor (DuckDB) para BI y Chatbot** |
| **Tiempo de Respuesta** | Minutos en almacenes relacionales pesados | **Milisegundos** gracias a DuckDB + Parquet |
| **Privacidad** | Los datos viajan a terceros para inferencia | **Soberanía total** on-premise / privada |
| **Gobernanza** | Linaje manual o inexistente | **Linaje automático** en OpenMetadata/Atlas |

*Notas del Orador:*
*En resumen: logramos una arquitectura de vanguardia, con costos operativos mínimos, privacidad asegurada para el negocio y una experiencia de usuario inmediata.*

---

<!-- Slide 12: Conclusiones y Próximos Pasos -->
# Conclusión y Roadmap
## Democratización analítica con rigor técnico y gobernanza sólida

### Logros Consolidados:
- Pipeline Medallion funcional con aislamiento en cuarentena.
- Capa de servicio rápida, confiable y segura con DuckDB.
- Chatbot analítico con IA local (Qwen 2.5), códigos de error y cero respuestas enlatadas.
- 100% verificado mediante pruebas automatizadas y documentación viva.

### Próximos Pasos:
1. Ingesta continua en tiempo real mediante streaming (Kafka / Spark Streaming).
2. Expansión del catálogo de gobernanza con alertas automáticas de desviación de calidad (Data Drift).
3. Afinación de prompts especializados para análisis predictivos avanzados.

---

# ¡Gracias!
## Preguntas y Discusión (Q&A)

**Repositorio del Proyecto:** Pipeline-analitica  
**Contacto y Equipo:** Equipo de Ingeniería de Datos y Analítica (7 Integrantes)  
**Documentación completa:** `documentation/arquitectura-medallion.md`

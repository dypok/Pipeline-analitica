---
marp: true
html: true
theme: default
paginate: true
size: 16:9
style: |
  section {
    background-color: #0b1120;
    color: #f1f5f9;
    font-family: 'Inter', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    padding: 28px 48px;
    font-size: 0.95rem;
  }
  h1 {
    color: #38bdf8;
    font-size: 1.6rem;
    font-weight: 800;
    margin-top: 0;
    margin-bottom: 0.2rem;
    letter-spacing: -0.02em;
  }
  h2 {
    color: #94a3b8;
    font-size: 1.05rem;
    font-weight: 500;
    margin-top: 0;
    margin-bottom: 12px;
    line-height: 1.35;
  }
  h3 {
    color: #e2e8f0;
    font-size: 0.95rem;
    margin-top: 0;
    margin-bottom: 0.35rem;
    font-weight: 700;
  }
  p, li {
    font-size: 0.82rem;
    line-height: 1.45;
    color: #cbd5e1;
  }
  ul {
    margin-top: 6px;
    margin-bottom: 8px;
    padding-left: 20px;
  }
  li {
    margin-bottom: 5px;
  }
  .highlight {
    color: #38bdf8;
    font-weight: 600;
  }
  .card-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 16px;
    margin-top: 10px;
  }
  .card-grid-2 {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 18px;
    margin-top: 10px;
  }
  .card {
    background: #151f32;
    border: 1px solid #293548;
    border-radius: 10px;
    padding: 16px 18px;
  }
  .card-header {
    display: flex;
    align-items: center;
    margin-bottom: 8px;
  }
  .icon-box {
    width: 28px;
    height: 28px;
    border-radius: 6px;
    display: flex;
    align-items: center;
    justify-content: center;
    margin-right: 10px;
    flex-shrink: 0;
  }
  .badge {
    display: inline-block;
    padding: 3px 10px;
    border-radius: 9999px;
    font-size: 0.72rem;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }
  table {
    width: 100%;
    border-collapse: collapse;
    margin-top: 10px;
    font-size: 0.8rem;
  }
  th {
    background-color: #1e293b;
    color: #38bdf8;
    font-weight: 700;
    text-align: left;
    padding: 8px 12px;
    border: 1px solid #334155;
  }
  td {
    background-color: #0f172a;
    color: #cbd5e1;
    padding: 8px 12px;
    border: 1px solid #1e293b;
  }
  tr:nth-child(even) td {
    background-color: #162032;
  }
  code {
    background: #1e293b;
    color: #38bdf8;
    padding: 2px 6px;
    border-radius: 4px;
    font-size: 0.8rem;
    font-family: monospace;
  }
  footer {
    color: #64748b;
    font-size: 0.75rem;
  }
---

<!-- Slide 1: Portada -->
<div style="height: 100%; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center;">
  <div style="margin-bottom: 16px;">
    <span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.3);">
      ARQUITECTURA LAKEHOUSE • MEDALLION 2.0 • IA SOBERANA LOCAL
    </span>
  </div>
  
  <h1 style="font-size: 2.2rem; color: #ffffff; margin-bottom: 8px;">
    Plataforma Extensible de <span style="color: #38bdf8;">Datos y Analítica</span>
  </h1>
  
  <h2 style="font-size: 1.15rem; color: #94a3b8; max-width: 800px; margin: 0 auto 24px auto;">
    Ingeniería de datos integral con MinIO, Apache Spark, aceleración analítica con DuckDB y Chatbot con LLM Soberano (Qwen 2.5) sin costes de API.
  </h2>

  <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 14px; max-width: 820px; width: 100%; text-align: left;">
    <div style="background: #151f32; border: 1px solid #293548; padding: 10px 14px; border-radius: 8px;">
      <div style="font-size: 0.68rem; color: #64748b; text-transform: uppercase; font-weight: 700;">Almacenamiento</div>
      <div style="font-size: 0.9rem; font-weight: 700; color: #38bdf8; margin-top: 2px;">MinIO S3 Lakehouse</div>
    </div>
    <div style="background: #151f32; border: 1px solid #293548; padding: 10px 14px; border-radius: 8px;">
      <div style="font-size: 0.68rem; color: #64748b; text-transform: uppercase; font-weight: 700;">Procesamiento</div>
      <div style="font-size: 0.9rem; font-weight: 700; color: #a855f7; margin-top: 2px;">Apache Spark 3.5</div>
    </div>
    <div style="background: #151f32; border: 1px solid #293548; padding: 10px 14px; border-radius: 8px;">
      <div style="font-size: 0.68rem; color: #64748b; text-transform: uppercase; font-weight: 700;">Motor Analítico</div>
      <div style="font-size: 0.9rem; font-weight: 700; color: #34d399; margin-top: 2px;">DuckDB + Parquet</div>
    </div>
    <div style="background: #151f32; border: 1px solid #293548; padding: 10px 14px; border-radius: 8px;">
      <div style="font-size: 0.68rem; color: #64748b; text-transform: uppercase; font-weight: 700;">Inteligencia Artificial</div>
      <div style="font-size: 0.9rem; font-weight: 700; color: #fbbf24; margin-top: 2px;">Qwen 2.5 (2B Local)</div>
    </div>
  </div>
</div>

<!--
Notas del Orador:
Buenos días a todos. Hoy presentamos la Plataforma Extensible de Datos, una solución integral diseñada para capturar, procesar, certificar y consultar información empresarial a escala sin fricciones, integrando dashboards visuales y consultas en lenguaje natural con IA soberana y sin costes de API.
-->

---

<!-- Slide 2: El Problema y Desafíos Actuales -->
# El Problema y Desafíos Actuales
## ¿Por qué las organizaciones no confían en sus datos ni aprovechan la IA analítica?

<div class="card-grid">
  <div class="card" style="border-top: 3px solid #f87171;">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(248, 113, 113, 0.15);">
        <img src="./images/icons/alert.svg" width="16" height="16" alt="alerta" />
      </div>
      <h3 style="color: #f87171; margin: 0;">Silos y Formatos Dispersos</h3>
    </div>
    <p style="margin-top: 6px; color: #94a3b8;">
      Datos fragmentados en archivos CSV manuales, JSON sin esquema validado y APIs externas sin contrato común de ingesta ni tipado estricto.
    </p>
  </div>

  <div class="card" style="border-top: 3px solid #fbbf24;">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(251, 191, 36, 0.15);">
        <img src="./images/icons/search.svg" width="16" height="16" alt="linaje" />
      </div>
      <h3 style="color: #fbbf24; margin: 0;">Linaje Opaco y Calidad Nula</h3>
    </div>
    <p style="margin-top: 6px; color: #94a3b8;">
      Falta total de trazabilidad: cuando una métrica falla, es imposible identificar el origen. Registros corruptos contaminan los reportes ejecutivos.
    </p>
  </div>

  <div class="card" style="border-top: 3px solid #a855f7;">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(168, 85, 247, 0.15);">
        <img src="./images/icons/cpu.svg" width="16" height="16" alt="ia" />
      </div>
      <h3 style="color: #c084fc; margin: 0;">Alucinación y Riesgo en IA</h3>
    </div>
    <p style="margin-top: 6px; color: #94a3b8;">
      Los chatbots convencionales inventan números de negocio o filtran información privada a APIs en la nube con altos costos por token.
    </p>
  </div>
</div>

<!--
Notas del Orador:
Las empresas enfrentan tres grandes dolores: datos desarticulados, falta de confianza en los números por carecer de linaje y calidad, y la adopción deficiente de IA por alucinaciones y altos costos en la nube.
-->

---

<!-- Slide 3: La Propuesta de Valor -->
# Propuesta de Valor y Principios de Diseño
## Consistencia garantizada: Una única fuente de verdad (Single Source of Truth)

<div class="card-grid-2">
  <div class="card" style="border-left: 4px solid #38bdf8;">
    <span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; margin-bottom: 8px;">
      PRINCIPIO 1: ARQUITECTURA LAKEHOUSE MEDALLION
    </span>
    <h3 style="color: #ffffff; margin-top: 6px;">Tratamiento Secuencial por Madurez</h3>
    <ul>
      <li>Flujo desacoplado en capas: <b>Landing</b>, <b>Bronze</b>, <b>Silver</b>, <b>Gold</b> y <b>Cuarentena</b>.</li>
      <li><b>Procesamiento Idempotente:</b> Apache Spark asegura reprocesamiento sin duplicar filas.</li>
      <li><b>Aislamiento Activo:</b> Datos corruptos no bloquean la ingesta de registros limpios.</li>
    </ul>
  </div>

  <div class="card" style="border-left: 4px solid #34d399;">
    <span class="badge" style="background: rgba(52, 211, 153, 0.15); color: #34d399; margin-bottom: 8px;">
      PRINCIPIO 2: CONSUMO CONSISTENTE Y SOBERANO
    </span>
    <h3 style="color: #ffffff; margin-top: 6px;">Mismas Métricas para Dashboard y Chatbot</h3>
    <ul>
      <li><b>Doble Canal Unificado:</b> Streamlit (BI visual) y Chatbot IA consultan la misma capa Gold.</li>
      <li><b>Cero Alucinaciones:</b> El chatbot traduce preguntas a sentencias SQL reales ejecutadas con DuckDB.</li>
      <li><b>100% Soberanía y $0 Inferencia:</b> Modelo Qwen 2.5 local sin envíos a APIs externas.</li>
    </ul>
  </div>
</div>

<!--
Notas del Orador:
Nuestra premisa fundamental es que el dashboard y el chatbot consumen exactamente los mismos datos y las mismas fórmulas. El chatbot nunca inventa números: traduce preguntas a consultas SQL seguras que se ejecutan directamente sobre los datos certificados.
-->

---

<!-- Slide 4: Arquitectura General de la Solución -->
# Arquitectura End-to-End
## De la ingesta heterogénea al valor analítico accionable

<div style="text-align: center; margin-top: 6px;">
  <img src="./images/arquitectura_lakehouse.svg" style="width: 870px; max-height: 420px; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.4);" alt="Arquitectura End-to-End" />
</div>

<!--
Notas del Orador:
Aquí observamos el flujo completo de la plataforma. La ingesta alimenta el almacenamiento en MinIO; Spark procesa y clasifica lo válido a Silver y lo erróneo a Quarantine; Gold almacena Data Marts en formato Parquet, que son consumidos a velocidad sub-segundo por DuckDB tanto para Streamlit como para el Chatbot.
-->

---

<!-- Slide 5: Detalle de Capas Medallion -->
# Arquitectura Medallion al Detalle
## Trazabilidad y elevación progresiva de la calidad del dato

<div style="text-align: center; margin-top: 6px;">
  <img src="./images/medallion_pipeline.svg" style="width: 870px; max-height: 420px; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.4);" alt="Pipeline Medallion" />
</div>

<!--
Notas del Orador:
Cada capa tiene un rol claro. Una característica clave es la zona de Cuarentena: durante el proceso de limpieza y validación de calidad entre Bronze y Silver, si un registro viene con valores negativos o fechas inválidas, no aborta el lote completo ni entra a Silver; se desvía a Cuarentena con su error_code para auditoría y posterior reintento.
-->

---

<!-- Slide 6: Modelos Analíticos en Capa Gold -->
# Capa Gold: Modelos de Negocio
## Data Marts optimizados para consumo inmediato en Dashboards y Chatbot

<div style="text-align: center; margin-top: 6px;">
  <img src="./images/grafico_modelos_gold.svg" style="width: 870px; max-height: 420px; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.4);" alt="Modelos Analíticos Gold" />
</div>

<!--
Notas del Orador:
Estos tres modelos resuelven más del 90% de las preguntas frecuentes de negocio: cuánto vendemos (daily_sales), quién nos compra y qué clientes están en riesgo (customer_360) y qué productos generan mayor margen (product_performance).
-->

---

<!-- Slide 7: Motor de Consulta DuckDB & Dashboards -->
# Visualización e Hiper-Rendimiento
## DuckDB + Streamlit: Analítica interactiva en milisegundos

<div class="card-grid-2">
  <div class="card">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(52, 211, 153, 0.15);">
        <img src="./images/icons/zap.svg" width="16" height="16" alt="duckdb" />
      </div>
      <h3 style="color: #34d399; margin: 0;">DuckDB como Motor Analítico Embebido</h3>
    </div>
    <ul>
      <li><b>Motor Vectorizado Columnar:</b> Ejecución SQL de ultra-alta velocidad directamente en memoria.</li>
      <li><b>Zero-Copy sobre Parquet:</b> Lee directo sobre la capa Gold sin requerir servidores intermediarios.</li>
      <li><b>Latencia Sub-Segundo:</b> Consultas analíticas complejas en menos de 150 ms.</li>
      <li><b>Coste Cero de Infraestructura:</b> Sin costos de licenciamiento ni clústeres dedicados de bases de datos.</li>
    </ul>
  </div>

  <div class="card">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(56, 189, 248, 0.15);">
        <img src="./images/icons/dashboard.svg" width="16" height="16" alt="dashboard" />
      </div>
      <h3 style="color: #38bdf8; margin: 0;">Dashboards Ejecutivos en Streamlit</h3>
    </div>
    <ul>
      <li><b>Vistas Multi-Rol:</b> Tableros estratégicos para directores y paneles operativos para supervisores.</li>
      <li><b>Visualizaciones Plotly:</b> Gráficos interactivos de líneas, barras y dispersión en tiempo real.</li>
      <li><b>Filtros Reactivos:</b> Segmentación dinámica por rango temporal, categorías y canales.</li>
      <li><b>Seguridad por Roles:</b> Control de acceso basado en credenciales empresariales.</li>
    </ul>
  </div>
</div>

<!--
Notas del Orador:
Al usar DuckDB directamente sobre los archivos Parquet de la capa Gold, eliminamos la necesidad de mantener costosos data warehouses en la nube para el consumo inicial, logrando latencias de consulta inferiores a 200 ms.
-->

---

<!-- Slide 8: Chatbot con IA Local y Cero Fallbacks -->
# Chatbot Analítico Inteligente
## NL-to-SQL Soberano: Modelo Qwen 2.5 (2B) sin dependencias externas

<div style="text-align: center; margin-top: 6px;">
  <img src="./images/chatbot_flow.svg" style="width: 870px; max-height: 420px; border-radius: 8px; box-shadow: 0 4px 20px rgba(0,0,0,0.4);" alt="Flujo Chatbot Soberano" />
</div>

<!--
Notas del Orador:
El usuario pregunta en español cotidiano. El modelo local Qwen2.5 genera la consulta SQL, SQLGuard verifica que sea segura y de solo lectura, DuckDB la ejecuta y el modelo sintetiza la respuesta. Si algo falla o no hay datos, se entrega un código de error preciso y transparente.
-->

---

<!-- Slide 9: Orquestación, Linaje y Gobernanza -->
# Gobernanza, Linaje y Operación Continua
## Control total de extremo a extremo: calidad, contratos y observabilidad

<div class="card-grid">
  <div class="card" style="border-top: 3px solid #3b82f6;">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(59, 130, 246, 0.15);">
        <img src="./images/icons/clock.svg" width="16" height="16" alt="airflow" />
      </div>
      <h3 style="color: #60a5fa; margin: 0;">Apache Airflow</h3>
    </div>
    <p style="color: #94a3b8; font-size: 0.8rem;">
      Orquestación programada de DAGs con validación de SLAs, gestión de reintentos idempotentes y registro de métricas operativas por lote.
    </p>
  </div>

  <div class="card" style="border-top: 3px solid #8b5cf6;">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(139, 92, 246, 0.15);">
        <img src="./images/icons/lineage.svg" width="16" height="16" alt="openmetadata" />
      </div>
      <h3 style="color: #c084fc; margin: 0;">OpenMetadata</h3>
    </div>
    <p style="color: #94a3b8; font-size: 0.8rem;">
      Mapeo visual del linaje de datos de extremo a extremo: trazabilidad desde el conector API de origen hasta el dashboard de Streamlit y el Chatbot.
    </p>
  </div>

  <div class="card" style="border-top: 3px solid #ec4899;">
    <div class="card-header">
      <div class="icon-box" style="background: rgba(236, 72, 153, 0.15);">
        <img src="./images/icons/shield.svg" width="16" height="16" alt="atlas" />
      </div>
      <h3 style="color: #f472b6; margin: 0;">Apache Atlas</h3>
    </div>
    <p style="color: #94a3b8; font-size: 0.8rem;">
      Catálogo empresarial de metadatos, clasificación de información confidencial (PII) y auditoría de políticas de acceso al lago de datos.
    </p>
  </div>
</div>

<!--
Notas del Orador:
La gobernanza no es un añadido opcional; está integrada en el diseño. Podemos saber con exactitud de qué tabla y de qué archivo se originó cada cálculo que ve el usuario final.
-->

---

<!-- Slide 10: Estrategia de Entrega y Calidad de Software -->
# Entrega Técnica y Pruebas Rigurosas
## Metodología Mock-First y Suite Automatizada con 82 Pruebas

<div class="card-grid">
  <div class="card">
    <span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; margin-bottom: 6px;">DESARROLLO DESACOPLADO</span>
    <h3 style="color: #ffffff; margin-top: 4px;">Metodología Mock-First</h3>
    <p style="color: #94a3b8; font-size: 0.8rem;">
      Generación de datos sintéticos controlados para habilitar el avance en paralelo de los 7 roles sin cuellos de botella de integración.
    </p>
  </div>

  <div class="card">
    <span class="badge" style="background: rgba(251, 191, 36, 0.15); color: #fbbf24; margin-bottom: 6px;">COBERTURA FUNCIONAL</span>
    <h3 style="color: #ffffff; margin-top: 4px;">Matriz P01 a P12</h3>
    <p style="color: #94a3b8; font-size: 0.8rem;">
      Pruebas formales de contratos de esquema, idempotencia Spark, aislamiento estricto en Cuarentena y seguridad AST en SQLGuard.
    </p>
  </div>

  <div class="card">
    <span class="badge" style="background: rgba(52, 211, 153, 0.15); color: #34d399; margin-bottom: 6px;">VELOCIDAD CI/CD</span>
    <h3 style="color: #ffffff; margin-top: 4px;">82 Tests en &lt; 4 Segundos</h3>
    <p style="color: #94a3b8; font-size: 0.8rem;">
      Suite automatizada completa ejecutada con Pytest en hardware convencional, garantizando calidad continua y cero regresiones.
    </p>
  </div>
</div>

<!--
Notas del Orador:
Para evitar que el equipo de visualización esperara a que el equipo de ingesta terminara, implementamos generadores de datos sintéticos consistentes. Además, la suite de 82 pruebas garantiza que cada pull request llegue verificado y seguro a producción.
-->

---

<!-- Slide 11: Resumen de Beneficios e Impacto -->
# Impacto y Ventajas Competitivas
## Comparativa: Arquitectura Tradicional vs Plataforma Extensible de Datos

<table>
  <thead>
    <tr>
      <th style="width: 22%;">Dimensión</th>
      <th style="width: 39%;">Enfoque Tradicional en la Nube</th>
      <th style="width: 39%; color: #34d399;">Nuestra Plataforma Integrada</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td><b>Costos de Inferencia IA</b></td>
      <td>Facturación recurrente por token (OpenAI/Anthropic)</td>
      <td><span style="color: #34d399; font-weight: 700;">$0 en inferencia:</span> Modelo 2B local soberano</td>
    </tr>
    <tr>
      <td><b>Confiabilidad de Datos</b></td>
      <td>Métricas discordantes entre BI y chatbots</td>
      <td><span style="color: #38bdf8; font-weight: 700;">Mismo motor (DuckDB)</span> para Dashboard y Bot</td>
    </tr>
    <tr>
      <td><b>Latencia de Consulta</b></td>
      <td>Varios segundos/minutos en Data Warehouses pesados</td>
      <td><span style="color: #34d399; font-weight: 700;">Sub-segundo (&lt; 150 ms)</span> con DuckDB + Parquet</td>
    </tr>
    <tr>
      <td><b>Privacidad y Seguridad</b></td>
      <td>Datos sensibles viajan a APIs de terceros</td>
      <td><span style="color: #38bdf8; font-weight: 700;">Soberanía total on-premise</span> o nube privada</td>
    </tr>
    <tr>
      <td><b>Gobernanza y Linaje</b></td>
      <td>Trazabilidad manual o nula ante fallos</td>
      <td><span style="color: #c084fc; font-weight: 700;">Linaje automatizado</span> con OpenMetadata y Atlas</td>
    </tr>
  </tbody>
</table>

<!--
Notas del Orador:
En resumen: logramos una arquitectura de vanguardia, con costos operativos mínimos, privacidad asegurada para el negocio y una experiencia de usuario inmediata.
-->

---

<!-- Slide 12: Conclusiones y Próximos Pasos -->
# Conclusión y Roadmap Futuro
## Democratización analítica con rigor técnico, privacidad y gobernanza

<div class="card-grid-2">
  <div class="card" style="border-left: 4px solid #34d399;">
    <h3 style="color: #34d399;">Logros Consolidados en v2.0</h3>
    <ul>
      <li>Pipeline Lakehouse Medallion con zona activa de Cuarentena.</li>
      <li>Capa de servicio analítica de alta velocidad con DuckDB embebido.</li>
      <li>Chatbot NL-to-SQL con IA local soberana, catálogo de errores y cero fallbacks.</li>
      <li>100% verificado: 82 pruebas automáticas y documentación viva.</li>
    </ul>
  </div>

  <div class="card" style="border-left: 4px solid #38bdf8;">
    <h3 style="color: #38bdf8;">Próximos Pasos en el Roadmap</h3>
    <ul>
      <li><b>Streaming en Tiempo Real:</b> Ingesta continua con Apache Kafka y Spark Streaming.</li>
      <li><b>Detección de Data Drift:</b> Alertas automáticas de desviación de calidad en esquemas.</li>
      <li><b>Análisis Predictivo:</b> Inferencia de series temporales integrada en el Chatbot.</li>
    </ul>
  </div>
</div>

<!--
Notas del Orador:
Concluimos con una plataforma robusta y lista para producción, y un roadmap claro hacia streaming y detección automática de desviaciones de datos.
-->

---

<!-- Slide 13: Cierre y Preguntas -->
<div style="height: 100%; display: flex; flex-direction: column; justify-content: center; align-items: center; text-align: center;">
  <span class="badge" style="background: rgba(56, 189, 248, 0.15); color: #38bdf8; margin-bottom: 14px;">
    PIPELINE ANALÍTICA • OCTUBRE 2026
  </span>

  <h1 style="font-size: 2.4rem; color: #ffffff; margin-bottom: 8px;">
    ¡Muchas Gracias!
  </h1>

  <h2 style="font-size: 1.15rem; color: #38bdf8; margin-bottom: 24px;">
    Espacio Abierto para Preguntas y Discusión Técnica
  </h2>

  <div style="background: #151f32; border: 1px solid #293548; padding: 16px 24px; border-radius: 10px; max-width: 600px; width: 100%; text-align: left;">
    <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 6px;">
      <b>Equipo del Proyecto:</b> Ingeniería de Datos y Analítica (7 Integrantes)
    </div>
    <div style="color: #94a3b8; font-size: 0.85rem; margin-bottom: 6px;">
      <b>Repositorio:</b> <code>Pipeline-analitica</code> (Branch: <code>develop</code>)
    </div>
    <div style="color: #94a3b8; font-size: 0.85rem;">
      <b>Documentación Técnica:</b> <code>documentation/arquitectura-medallion.md</code>
    </div>
  </div>
</div>

<!--
Notas del Orador:
Agradecemos la atención de todos y abrimos el espacio para preguntas, comentarios y discusión técnica sobre la implementación de la plataforma.
-->

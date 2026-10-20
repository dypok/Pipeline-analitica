## Descripción del Cambio
<!-- Explica de forma concisa qué problema resuelve o qué funcionalidad agrega este Pull Request -->

## Rama de Origen y Destino
- **Origen:** `feature/<nombre-de-tu-rama>`
- **Destino:** `develop`

## Capa / Componente Afectado
- [ ] Ingesta / Conectores (`src/ingestion`) — Jeffry
- [ ] Procesamiento Spark / Medallion (`src/silver`, `src/gold`) — Esteban Padilla
- [ ] Orquestación Airflow (`dags/`) — Héctor
- [ ] Calidad de Datos & Gobierno (`src/quality`, OpenMetadata) — Sebastián Ortiz
- [ ] Dashboards & Visualización (`dashboards/`) — Sergio
- [ ] Chatbot de IA & Consultas (`chatbot/`) — Dylan Gamero
- [ ] Infraestructura & Base Técnica (`infrastructure/`, `config/`) — Luis Gómez

## Lista de Verificación (Checklist)
- [ ] Mi código sigue las convenciones de estilo y tipos del proyecto.
- [ ] No incluí credenciales, secretos ni archivos `.env` en los commits.
- [ ] He actualizado los contratos de datos en `docs/contratos_integracion.md` si modifiqué algún esquema.
- [ ] Todas las pruebas unitarias pasan exitosamente (`pytest -v`).
- [ ] Probé el flujo de extremo a extremo localmente o en el contenedor correspondiente.
- [ ] Los nombres de archivos y buckets respetan las convenciones acordadas.

## Evidencia de Pruebas
<!-- Adjunta comandos ejecutados, logs de éxito o capturas de pantalla de la salida -->
```bash
pytest -v
```

## Notas para los Revisores
<!-- Instrucciones especiales para probar este cambio -->

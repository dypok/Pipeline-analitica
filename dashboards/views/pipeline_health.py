"""Pestaña 'Salud del Pipeline' (US-15).

Muestra:
    - Tarjetas con volumen de registros procesados en Landing, Bronze, Silver
      y Gold (vía ``analytics.pipeline_execution_logs``).
    - Tabla de registros rechazados en Quarantine con visualización de los
      motivos de error (``ERR_NULL_KEY``, ``ERR_INVALID_DATE``, etc.).
"""

from __future__ import annotations

import pandas as pd
import streamlit as st


def render_pipeline_health(
    executions: pd.DataFrame,
    task_metrics: pd.DataFrame,
    quarantine: pd.DataFrame,
    source_label: str | None = None,
) -> None:
    """Renderiza la pestaña 'Salud del Pipeline'."""
    st.header("🩺 Salud del Pipeline")
    if source_label:
        st.caption(f"Fuente de datos: `{source_label}`")

    # --- Volumen por capa -------------------------------------------------
    st.subheader("Volumen de registros procesados por capa")
    if executions.empty:
        st.info(
            "No hay ejecuciones registradas en ``analytics.pipeline_execution_logs``. "
            "Los registros aparecerán aquí en cuanto Héctor dispare el DAG maestro "
            "(US-10) o se corra ``scripts/run_pipeline_local.sh``."
        )
    else:
        layer_df = (
            executions.groupby("layer", as_index=False)
            .agg(
                ejecuciones=("execution_id", "count"),
                registros_procesados=("records_processed", "sum"),
                en_cuarentena=("records_quarantined", "sum"),
                exitosas=("status", lambda s: (s == "SUCCESS").sum()),
            )
        )
        cols = st.columns(len(layer_df))
        for col, (_, row) in zip(cols, layer_df.iterrows()):
            with col:
                st.metric(
                    label=str(row["layer"]).title(),
                    value=f"{int(row['registros_procesados']):,}",
                    delta=f"Cuarentena: {int(row['en_cuarentena']):,}",
                )

        st.markdown("---")
        st.subheader("Últimas ejecuciones")
        st.dataframe(executions.sort_values("finished_at", ascending=False).head(20),
                     width="stretch", hide_index=True)

    # --- Métricas operativas por tarea -----------------------------------
    if not task_metrics.empty:
        st.markdown("---")
        st.subheader("Métricas por tarea")
        st.dataframe(task_metrics, width="stretch", hide_index=True)

    # --- Registros en Quarantine -----------------------------------------
    st.markdown("---")
    st.subheader("Registros rechazados (Quarantine)")
    if quarantine.empty:
        st.info(
            "No hay registros en Quarantine disponibles. Esto puede significar "
            "que no se han rechazado filas (caso ideal) o que el motor de calidad "
            "de Sebastián (US-08) aún no ha publicado la zona. Cuando esté "
            "disponible, aquí verás la tabla con los motivos de error."
        )
    else:
        c1, c2, c3 = st.columns(3)
        c1.metric("Total en Quarantine", f"{len(quarantine):,}")
        if "entity_type" in quarantine.columns:
            c2.metric("Entidades afectadas", quarantine["entity_type"].nunique())
        if "quarantine_reason" in quarantine.columns:
            c3.metric("Motivos distintos", quarantine["quarantine_reason"].nunique())

        st.markdown("---")
        if "quarantine_reason" in quarantine.columns:
            st.markdown("**Distribución por motivo de rechazo**")
            reason_counts = quarantine["quarantine_reason"].value_counts().reset_index()
            reason_counts.columns = ["Motivo", "Cantidad"]
            st.dataframe(reason_counts, width="stretch", hide_index=True)

        st.markdown("**Detalle de registros rechazados**")
        st.dataframe(quarantine, width="stretch", hide_index=True)

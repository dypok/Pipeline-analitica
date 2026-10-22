"""Pestaña 'Salud del Pipeline' (US-15) — tema Oscuro Premium.

Muestra:
    - Volumen de registros por capa (Landing→Gold) con badges de estado.
    - Últimas ejecuciones y métricas por tarea.
    - Registros rechazados en Quarantine (cuando Sebastián publique US-08).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.components.download import render_download_button
from dashboards.components.pipeline_viz import render_pipeline_viz
from dashboards.styles import PLOTLY_CONFIG, PLOTLY_TEMPLATE


def _apply_template(fig):
    return fig.update_layout(template=PLOTLY_TEMPLATE)


def _status_badge(status: str) -> str:
    s = str(status).upper()
    if s == "SUCCESS":
        return '<span class="badge ok">SUCCESS</span>'
    if s in ("FAILED", "ERROR"):
        return '<span class="badge error">FAILED</span>'
    return f'<span class="badge warn">{s}</span>'


def render_pipeline_health(
    executions: pd.DataFrame,
    task_metrics: pd.DataFrame,
    quarantine: pd.DataFrame,
    source_label: str | None = None,
) -> None:
    """Renderiza la página 'Salud del Pipeline'."""
    st.header("Salud del Pipeline")
    if source_label:
        st.caption(f"Fuente de datos: `{source_label}`")

    # --- Grafo de nodos Landing → Gold (estilo DataOps) ---------------------
    render_pipeline_viz(executions, task_metrics)
    st.markdown("---")

    # --- Volumen por capa ---------------------------------------------------
    st.subheader("Volumen de registros procesados por capa")
    if executions.empty:
        st.info(
            "No hay ejecuciones en `analytics.pipeline_execution_logs`. "
            "Aparecerán cuando Héctor dispare el DAG maestro (US-10) o se corra "
            "`scripts/run_pipeline_local.sh`."
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
        # Gráfico de barras del volumen por capa.
        fig_layers = px.bar(
            layer_df,
            x="layer",
            y="registros_procesados",
            title="Registros Procesados por Capa",
            labels={"layer": "Capa", "registros_procesados": "Registros"},
            color="registros_procesados",
            color_continuous_scale="YlGnBu",
        )
        fig_layers.update_layout(coloraxis_showscale=False)
        _apply_template(fig_layers)
        st.plotly_chart(fig_layers, width="stretch", config=PLOTLY_CONFIG, key="layers_chart")

        st.markdown("### Últimas ejecuciones")
        # Renderiza badges de estado en la tabla de ejecuciones.
        display = executions.sort_values("finished_at", ascending=False).head(20).copy()
        if "status" in display.columns:
            display["estado_html"] = display["status"].map(_status_badge)
        st.dataframe(display, width="stretch", hide_index=True)

    # --- Métricas operativas por tarea ---------------------------------------
    if not task_metrics.empty:
        st.markdown("---")
        st.subheader("Métricas por tarea")
        st.dataframe(task_metrics, width="stretch", hide_index=True)

    # --- Registros en Quarantine ----------------------------------------------
    st.markdown("---")
    st.subheader("Registros rechazados (Quarantine)")
    if quarantine.empty:
        st.info(
            "Sin registros en Quarantine. Puede significar que no se rechazó "
            "ninguna fila (ideal) o que el motor de calidad de Sebastián (US-08) "
            "aún no publicó la zona."
        )
    else:
        from dashboards.components.kpi_cards import render_kpi_cards

        render_kpi_cards(
            {"quarantine_total": len(quarantine)},
            columns=["quarantine_total"],
        )
        st.markdown("")
        if "quarantine_reason" in quarantine.columns:
            st.markdown("**Distribución por motivo de rechazo**")
            reason_counts = quarantine["quarantine_reason"].value_counts().reset_index()
            reason_counts.columns = ["Motivo", "Cantidad"]
            fig_reason = px.bar(
                reason_counts,
                x="Cantidad",
                y="Motivo",
                orientation="h",
                title="Rechazos por Motivo",
                color="Cantidad",
                color_continuous_scale=["#6b2a2a", "#F87171"],
            )
            fig_reason.update_layout(coloraxis_showscale=False)
            _apply_template(fig_reason)
            st.plotly_chart(fig_reason, width="stretch", config=PLOTLY_CONFIG, key="reason_chart")

        st.markdown("**Detalle de registros rechazados**")
        render_download_button(quarantine, "Descargar quarantine (CSV)", "quarantine", key="dl_quarantine")
        st.dataframe(quarantine, width="stretch", hide_index=True)

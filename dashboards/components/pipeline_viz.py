"""Visualización de pipelines como grafo de red — Data Observability.

Representa las capas Medallion (Landing → Bronze → Silver → Gold) como un
**grafo de red con nodos brillantes** (estilo "Real-Time Monitoring" de la
referencia), en lugar de cajas HTML. Cada nodo muestra su estado con color y
brillo; los enlaces conectan las capas. Un nodo "Quarantine" cuelga de Silver.

Consume ``analytics.pipeline_execution_logs``. Sin ejecuciones (modo mock),
degrada a un estado "inactivo" informativo. Permite seleccionar una capa para
ver su detalle.
"""

from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from dashboards.styles import PLOTLY_CONFIG, PLOTLY_TEMPLATE

# Coordenadas fijas del grafo (x, y) — layout horizontal + rama quarantine.
# Etiquetas cortas mono como "icono" de nodo (sin emojis).
_NODES = {
    "landing": ("Landing", "LD", 0.0, 0.5),
    "bronze":  ("Bronze",  "BZ", 1.0, 0.5),
    "silver":  ("Silver",  "SV", 2.0, 0.5),
    "gold":    ("Gold",    "GD", 3.0, 0.5),
    "quarantine": ("Quarantine", "QR", 2.0, -0.4),
}
_EDGES = [("landing", "bronze"), ("bronze", "silver"), ("silver", "gold"), ("silver", "quarantine")]

_COLOR = {"ok": "#22C55E", "run": "#00E5FF", "error": "#F43F5E", "idle": "#3D5470"}
_STATUS_MAP = {"SUCCESS": "ok", "RUNNING": "run", "FAILED": "error", "ERROR": "error"}


def _normalize(layer: str) -> str:
    l = str(layer).lower()
    for key in ("landing", "bronze", "silver", "gold", "quarantine"):
        if key in l:
            return key
    return l


def _latest_by_layer(executions: pd.DataFrame) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if executions.empty:
        return out
    df = executions.copy()
    if "finished_at" in df.columns:
        df = df.sort_values("finished_at")
    for _, row in df.iterrows():
        out[_normalize(row.get("layer", ""))] = {
            "status": _STATUS_MAP.get(str(row.get("status", "")).upper(), "idle"),
            "records": row.get("records_processed"),
            "quarantine": row.get("records_quarantined"),
            "finished_at": row.get("finished_at"),
            "execution_id": row.get("execution_id"),
            "raw_layer": row.get("layer"),
            "raw_status": row.get("status"),
        }
    return out


def _fmt_records(v) -> str:
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "—"


def _fmt_ts(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return pd.to_datetime(v).strftime("%d/%m %H:%M")
    except Exception:
        return str(v)


def _draw_graph(latest: dict[str, dict], has_data: bool) -> go.Figure:
    """Grafo de red con nodos brillantes (capas + quarantine)."""
    fig = go.Figure()

    # --- Enlaces (líneas cian tenues) ---
    for a, b in _EDGES:
        xa, ya, _ = _NODES[a][1], _NODES[a][3], None
        fig.add_trace(
            go.Scatter(
                x=[_NODES[a][2], _NODES[b][2]],
                y=[_NODES[a][3], _NODES[b][3]],
                mode="lines",
                line=dict(color="rgba(34,211,238,0.35)", width=2),
                hoverinfo="skip",
                showlegend=False,
))

    # --- Nodos: capa de brillo (grande, translúcida) + núcleo (pequeño, brillante) ---
    for key, (name, icon, x, y) in _NODES.items():
        if key == "quarantine":
            info = latest.get("quarantine")
            status = info["status"] if info else ("idle" if not has_data else "idle")
            # Quarantine solo se marca rojo si hay rechazos.
            if info and str(info.get("quarantine") or 0) not in ("0", "0.0", "None", ""):
                status = "error"
            elif info:
                status = "ok"
        else:
            info = latest.get(key)
            status = info["status"] if info else "idle"
        color = _COLOR.get(status, _COLOR["idle"])

        # Halo de brillo.
        fig.add_trace(
            go.Scatter(
                x=[x], y=[y], mode="markers",
                marker=dict(size=46, color=color, opacity=0.18),
                hoverinfo="skip", showlegend=False,
            )
        )
        # Núcleo brillante.
        fig.add_trace(
            go.Scatter(
                x=[x], y=[y], mode="markers+text",
                marker=dict(size=30, color=color, line=dict(width=2, color="#E8F6FF")),
                text=[icon], textposition="middle center",
                textfont=dict(color="#03060E", size=10, family="'JetBrains Mono', monospace"),
                hovertemplate=f"<b>{name}</b><extra></extra>",
                showlegend=False,
            )
        )
        # Etiqueta debajo del nodo.
        fig.add_trace(
            go.Scatter(
                x=[x], y=[y - 0.22], mode="text",
                text=[name], textfont=dict(color="#E8F6FF", size=12),
                hoverinfo="skip", showlegend=False,
            )
        )

    fig.update_layout(
        template=PLOTLY_TEMPLATE,
        height=280,
        showlegend=False,
        xaxis=dict(visible=False, range=[-0.5, 3.5]),
        yaxis=dict(visible=False, range=[-0.7, 0.85]),
        margin=dict(l=10, r=10, t=10, b=10),
    )
    return fig


def render_pipeline_viz(executions: pd.DataFrame, task_metrics=None) -> None:
    """Renderiza el grafo de red del pipeline y su panel de detalle."""
    st.markdown("### Flujo Medallion (Landing → Gold)")

    latest = _latest_by_layer(executions)
    has_data = bool(latest)

    st.plotly_chart(_draw_graph(latest, has_data), width="stretch", config=PLOTLY_CONFIG, key="pipe_graph")

    # --- Leyenda de estados ---
    st.markdown(
        '<span class="badge ok">● Completado</span>'
        '<span class="badge" style="color:#00E5FF;border-color:rgba(34,211,238,0.4)">● En ejecución</span>'
        '<span class="badge error">● Fallido</span>'
        '<span class="badge">● Inactivo</span>',
        unsafe_allow_html=True,
    )

    # --- Selección de capa para detalle ---
    labels = {v[0]: k for k, v in _NODES.items()}
    sel_name = st.selectbox("Selecciona una capa para ver su detalle", options=list(labels.keys()), key="pipe_sel")
    sel_key = labels[sel_name]

    st.markdown("---")
    st.markdown(f"#### Detalle: {sel_name}")

    if not has_data:
        st.info(
            "No hay ejecuciones en `analytics.pipeline_execution_logs`. Las capas "
            "mostrarán estado real al ejecutar el pipeline (`scripts/run_pipeline_local.sh`) "
            "o el DAG de Airflow (US-10). Hoy el dashboard opera en modo **mock local**."
        )
        return

    info = latest.get(sel_key)
    if not info:
        st.warning(f"La capa **{sel_name}** no tiene ejecuciones registradas aún.")
        return

    c1, c2, c3, c4 = st.columns(4)
    label = {"ok": "Completado", "run": "En ejecución", "error": "Fallido"}.get(info["status"], "Inactivo")
    c1.metric("Estado", label)
    c2.metric("Registros", _fmt_records(info["records"]))
    c3.metric("Cuarentena", _fmt_records(info["quarantine"]))
    c4.metric("Última ejecución", _fmt_ts(info["finished_at"]))
    st.caption(f"Capa: `{info['raw_layer']}` · Status: `{info['raw_status']}` · ID: `{info['execution_id']}`")

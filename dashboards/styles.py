"""Estilos del dashboard — tema "Data Observability" (cian neón sci-fi).

Referencia visual: dashboard de observabilidad de datos. Fondo casi negro
azulado, acento **cian monochromático** (sin violeta), gráficos de **área
rellena** con degradado, anillos radiales, grafo de red con nodos brillantes
y alertas con triángulos semánticos.

Ofrece:
    - ``apply_custom_css()``: CSS global (fondo casi negro + glow cian, tarjetas
      con borde cian, pills de estado, listas de estado/alerta, responsive y
      soporte de "prefers-reduced-motion").
    - ``build_plotly_template()`` / ``PLOTLY_TEMPLATE``: template Plotly con
      fondos transparentes, acentos cian y cuadrícula tenue.
    - Helpers de UI: ``kpi_card_html``, ``area_fill`` (áreas rellenas),
      ``status_item_html``, ``alert_item_html``, ``progress_bar_html``.

El tema base vive en ``.streamlit/config.toml``; aquí va el caparazón visual.
"""

from __future__ import annotations

import streamlit as st

# --- Paleta de marca (cian monochromático + semánticos) ---------------------
CYAN = "#22D3EE"
CYAN_BRIGHT = "#00E5FF"
GREEN = "#22C55E"
AMBER = "#F59E0B"
RED = "#F43F5E"

PALETTE: list[str] = [CYAN_BRIGHT, CYAN, "#67E8F9", "#A5F3FC", "#38BDF8", GREEN, AMBER, RED]

STATUS_COLORS = {"ok": GREEN, "running": CYAN, "warn": AMBER, "error": RED, "idle": "#64748B"}


def build_plotly_template() -> dict:
    """Template Plotly: fondo transparente, acento cian, cuadrícula tenue."""
    return {
        "layout": {
            "colorway": PALETTE,
            "paper_bgcolor": "rgba(0,0,0,0)",
            "plot_bgcolor": "rgba(0,0,0,0)",
            "font": {"color": "#E8F6FF", "family": "Inter, sans-serif", "size": 13},
            "title": {"font": {"size": 16, "color": "#E8F6FF"}, "x": 0.0, "xanchor": "left"},
            "xaxis": {
                "gridcolor": "rgba(120,180,220,0.08)",
                "zerolinecolor": "rgba(120,180,220,0.12)",
                "tickfont": {"color": "#7E9AB8"},
                "title": {"font": {"color": "#7E9AB8"}},
            },
            "yaxis": {
                "gridcolor": "rgba(120,180,220,0.08)",
                "zerolinecolor": "rgba(120,180,220,0.12)",
                "tickfont": {"color": "#7E9AB8"},
                "title": {"font": {"color": "#7E9AB8"}},
            },
            "legend": {"bgcolor": "rgba(0,0,0,0)", "font": {"color": "#E8F6FF"}},
            "hoverlabel": {"bgcolor": "#0A1220", "bordercolor": CYAN, "font": {"color": "#E8F6FF"}},
            "margin": {"l": 44, "r": 20, "t": 50, "b": 44},
        }
    }


PLOTLY_TEMPLATE: dict = build_plotly_template()

PLOTLY_CONFIG: dict = {
    "displaylogo": False,
    "modeBarButtonsToRemove": ["lasso2d", "select2d", "autoScale2d"],
    "responsive": True,
}


def area_fill(hex_color: str = CYAN_BRIGHT, opacity: float = 0.28) -> str:
    """Color de relleno semitransparente para gráficos de área (estilo obs.)."""
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i : i + 2], 16) for i in (0, 2, 4))
    return f"rgba({r},{g},{b},{opacity})"


def apply_custom_css() -> None:
    """Inyecta el CSS global Data Observability.

    Se inyecta en CADA run (sin cachear en session_state). Si se usa un flag
    para emitirlo "una sola vez", Streamlit elimina el bloque <style> del DOM
    en los reruns y todo el aspecto personalizado desaparece dejando HTML sin
    estilo. El costo de re-emitrir el CSS es despreciable.
    """
    st.markdown(
        """
        <style>
        /* ============ Base ============ */
        html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

        [data-testid="stAppViewContainer"] {
            background:
                radial-gradient(900px 420px at -6% -10%, rgba(34,211,238,0.10), transparent 55%),
                radial-gradient(800px 420px at 106% -8%, rgba(0,229,255,0.07), transparent 55%),
                #03060E;
        }

        /* ============ Tarjetas ============ */
        .card {
            background: rgba(10, 18, 32, 0.65);
            backdrop-filter: blur(8px);
            -webkit-backdrop-filter: blur(8px);
            border: 1px solid rgba(34, 211, 238, 0.16);
            border-radius: 14px;
            padding: 18px 20px;
            height: 100%;
            transition: border-color .2s ease, box-shadow .2s ease;
        }
        .card:hover {
            border-color: rgba(34, 211, 238, 0.45);
            box-shadow: 0 0 22px rgba(34, 211, 238, 0.10);
        }

        /* Tarjeta KPI */
        .kpi { display: flex; flex-direction: column; gap: 12px; }
        .kpi-top { display: flex; align-items: center; justify-content: space-between; }
        .kpi-label { color: #7E9AB8; font-size: 0.76rem; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }
        .kpi-bottom { display: flex; align-items: flex-end; justify-content: space-between; gap: 10px; }
        .kpi-value {
            font-size: 1.85rem; font-weight: 700; line-height: 1.05;
            color: #E8F6FF; text-shadow: 0 0 18px rgba(34,211,238,0.35);
        }
        .kpi-desc { color: #55708E; font-size: 0.73rem; margin-top: 2px; }
        .kpi-accent { height: 3px; border-radius: 3px; background: linear-gradient(90deg, #00E5FF, transparent); margin-top: 8px; }

        /* Pills de estado */
        .pill { display: inline-flex; align-items: center; gap: 4px; padding: 3px 9px;
                border-radius: 999px; font-size: 0.78rem; font-weight: 600; white-space: nowrap;
                border: 1px solid transparent; }
        .pill.ok   { background: rgba(34,197,94,0.12);  color: #22C55E; border-color: rgba(34,197,94,0.3); }
        .pill.warn { background: rgba(245,158,11,0.12); color: #F59E0B; border-color: rgba(245,158,11,0.3); }
        .pill.down { background: rgba(244,63,94,0.12);  color: #F43F5E; border-color: rgba(244,63,94,0.3); }
        .pill.flat { background: rgba(148,163,184,0.10); color: #94A3B8; border-color: rgba(148,163,184,0.25); }
        .pill.run  { background: rgba(34,211,238,0.12); color: #22D3EE; border-color: rgba(34,211,238,0.35); }

        /* ============ Sidebar ============ */
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #05090F 0%, #03060E 100%);
            border-right: 1px solid rgba(34,211,238,0.14);
        }
        [data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p { color: #B8CBDE; }
        [data-testid="stSidebarNav"] a[aria-current="page"] {
            background: linear-gradient(90deg, rgba(34,211,238,0.16), rgba(34,211,238,0.03));
            border: 1px solid rgba(34,211,238,0.4);
            border-radius: 11px;
            color: #E8F6FF !important;
            box-shadow: 0 0 16px rgba(34,211,238,0.12);
        }
        .menu-title { text-transform: uppercase; letter-spacing: 0.16em; font-size: 0.66rem;
                      color: #3D5470; margin: 14px 0 7px; }

        /* ============ Indicadores de servicio ============ */
        .svc { display: flex; align-items: center; justify-content: space-between;
               padding: 6px 11px; border-radius: 9px; margin: 3px 0;
               background: rgba(10,18,32,0.6); border: 1px solid rgba(34,211,238,0.10); }
        .svc-left { display: flex; align-items: center; gap: 9px; font-size: 0.8rem; color: #B8CBDE; }
        .svc-tag { font-family: 'JetBrains Mono', monospace; font-size: 0.66rem; font-weight: 600;
                   color: #22D3EE; background: rgba(34,211,238,0.10); border: 1px solid rgba(34,211,238,0.25);
                   border-radius: 5px; padding: 2px 6px; letter-spacing: 0.03em; }
        .dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; }
        .dot.ok  { background: #22C55E; box-shadow: 0 0 8px #22C55E; }
        .dot.err { background: #F43F5E; box-shadow: 0 0 8px #F43F5E; }

        /* ============ Listas de estado / alerta ============ */
        .status-item { display: flex; align-items: center; gap: 11px; padding: 9px 12px; border-radius: 10px;
                       background: rgba(10,18,32,0.55); border: 1px solid rgba(34,211,238,0.10); margin: 5px 0; }
        .status-ico { width: 30px; height: 30px; border-radius: 50%; flex: none;
                      display: flex; align-items: center; justify-content: center; font-size: 14px;
                      background: rgba(34,211,238,0.12); border: 1px solid rgba(34,211,238,0.25); }
        .status-name { color: #E8F6FF; font-size: 0.84rem; font-weight: 600; }
        .status-sub { color: #55708E; font-size: 0.72rem; }
        .alert-item { display: flex; align-items: flex-start; gap: 12px; padding: 12px 14px; border-radius: 12px;
                      background: rgba(10,18,32,0.6); border: 1px solid rgba(34,211,238,0.12); margin: 6px 0; }
        .alert-tri { width: 34px; height: 34px; border-radius: 9px; flex: none; display: flex;
                     align-items: center; justify-content: center; font-size: 17px; }
        .alert-tri.err { background: rgba(244,63,94,0.14); border: 1px solid rgba(244,63,94,0.4); }
        .alert-tri.ok  { background: rgba(34,197,94,0.14); border: 1px solid rgba(34,197,94,0.4); }
        .alert-tri.warn{ background: rgba(245,158,11,0.14); border: 1px solid rgba(245,158,11,0.4); }
        .alert-title { color: #E8F6FF; font-weight: 600; font-size: 0.86rem; }
        .alert-sub { color: #55708E; font-size: 0.76rem; line-height: 1.4; }
        .alert-val { color: #22D3EE; font-family: 'JetBrains Mono', monospace; font-size: 0.78rem; }

        /* ============ Barra de progreso ============ */
        .prog { background: rgba(34,211,238,0.10); border-radius: 999px; height: 7px; overflow: hidden; }
        .prog > div { height: 100%; border-radius: 999px; background: linear-gradient(90deg, #1B92A8, #00E5FF);
                      box-shadow: 0 0 10px rgba(34,211,238,0.4); }

        /* ============ Tablas / encabezados ============ */
        [data-testid="stDataFrame"] { border: 1px solid #16233C; border-radius: 12px; }
        h1, h2, h3 { color: #E8F6FF; letter-spacing: -0.01em; }
        h1 { font-weight: 700; }
        .badge { display: inline-block; padding: 3px 10px; border-radius: 999px; font-size: 0.72rem;
                 font-weight: 600; border: 1px solid rgba(34,211,238,0.25); background: rgba(10,18,32,0.6);
                 color: #B8CBDE; margin-right: 6px; }
        .badge.ok   { color: #22C55E; border-color: rgba(34,197,94,0.4); }
        .badge.warn { color: #F59E0B; border-color: rgba(245,158,11,0.4); }
        .badge.error{ color: #F43F5E; border-color: rgba(244,63,94,0.4); }
        hr { border-color: rgba(34,211,238,0.12); }

        /* ============ Animación de pulso ============ */
        @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.35; } }
        .pulse { animation: pulse 1.6s infinite; }

        /* ============ Responsive ============ */
        @media (max-width: 1024px) {
            [data-testid="stHorizontalBlock"] > div { min-width: 45% !important; }
            .kpi-value { font-size: 1.55rem; }
        }
        @media (max-width: 768px) {
            [data-testid="stHorizontalBlock"] { flex-wrap: wrap !important; }
            [data-testid="stHorizontalBlock"] > div { min-width: 100% !important; flex: 1 1 100% !important; }
            .kpi-value { font-size: 1.45rem; }
            h1 { font-size: 1.5rem !important; }
            [data-testid="stAppViewContainer"] { padding-top: 1rem; }
        }

        /* ============ Movimiento reducido ============ */
        @media (prefers-reduced-motion: reduce) {
            * { animation: none !important; transition: none !important; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def kpi_card_html(
    label: str,
    value: str,
    delta: str | None = None,
    direction: str = "flat",
    description: str | None = None,
) -> str:
    """Tarjeta KPI minimalista: etiqueta, valor con glow, pill y barra de acento.

    IMPORTANTE: se genera en UNA SOLA LÍNEA. ``st.markdown(unsafe_allow_html)``
    usa un parser markdown que, ante saltos de línea en blanco dentro del HTML,
    corta el bloque y escapa el resto como texto literal (bug visible como
    ``</div>`` suelto en la UI).
    """
    arrow = {"ok": "▲", "warn": "▲", "down": "▼", "flat": "•", "run": "●"}.get(direction, "•")
    pill_cls = {"ok": "ok", "down": "down", "warn": "warn", "run": "run"}.get(direction, "flat")
    pill = f'<span class="pill {pill_cls}">{arrow} {delta}</span>' if delta else ""
    desc = f'<div class="kpi-desc">{description}</div>' if description else ""
    return (
        f'<div class="card kpi">'
        f'<div class="kpi-top"><div class="kpi-label">{label}</div></div>'
        f'<div class="kpi-bottom"><div><div class="kpi-value">{value}</div>{desc}</div>{pill}</div>'
        f'<div class="kpi-accent"></div>'
        f'</div>'
    )


def service_indicator_html(name: str, tag: str, up: bool) -> str:
    """Fila de indicador de servicio en el sidebar (una sola línea)."""
    cls = "ok" if up else "err"
    estado = "conectado" if up else "caído"
    return (
        f'<div class="svc">'
        f'<div class="svc-left"><span class="svc-tag">{tag}</span><span>{name}</span></div>'
        f'<span class="dot {cls}" title="{estado}"></span>'
        f'</div>'
    )


def status_item_html(icon: str, name: str, subtitle: str = "") -> str:
    """Fila de lista de estado (una sola línea)."""
    sub = f'<div class="status-sub">{subtitle}</div>' if subtitle else ""
    return (
        f'<div class="status-item">'
        f'<div class="status-ico">{icon}</div>'
        f'<div><div class="status-name">{name}</div>{sub}</div>'
        f'</div>'
    )


def alert_item_html(kind: str, title: str, subtitle: str = "", value: str = "") -> str:
    """Fila de alerta con triángulo semántico (una sola línea)."""
    tri = {"err": "▲", "ok": "✓", "warn": "▲"}.get(kind, "▲")
    cls = kind if kind in ("err", "ok", "warn") else "warn"
    sub = f'<div class="alert-sub">{subtitle}</div>' if subtitle else ""
    val = f'<div class="alert-val">{value}</div>' if value else ""
    return (
        f'<div class="alert-item">'
        f'<div class="alert-tri {cls}">{tri}</div>'
        f'<div><div class="alert-title">{title}</div>{sub}{val}</div>'
        f'</div>'
    )


def progress_bar_html(label: str, pct: float, trailing: str = "") -> str:
    """Fila 'label + barra cian gradiente + %' (una sola línea)."""
    pct = max(0.0, min(100.0, pct))
    return (
        f'<div style="margin:9px 0;">'
        f'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:5px;">'
        f'<span style="color:#E8F6FF;font-weight:600;font-size:0.86rem;">{label}</span>'
        f'<span style="color:#22D3EE;font-weight:600;font-size:0.84rem;">{trailing}</span>'
        f'</div>'
        f'<div class="prog"><div style="width:{pct:.1f}%;"></div></div>'
        f'</div>'
    )

"""Página 'Clientes' — tema Data Observability (cian neón).

Contenido (espejo de la referencia visual):
    - Tarjetas KPI (Clientes, LTV, Órdenes/cliente).
    - "Clients geography": lista país + % + barra de progreso con gradiente.
    - Segmentación por LTV (barras).
    - Coroplético y tabla detallada + descarga.

Filtra por ``filters["countries"]`` (origen: ``customer_360.country``).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.components.download import render_download_button
from dashboards.components.kpi_cards import render_kpi_cards
from dashboards.kpi_calculator import compute_customer_segments, filter_by_selections
from dashboards.styles import PALETTE, PLOTLY_CONFIG, PLOTLY_TEMPLATE, progress_bar_html

# Mapping ISO-2 -> ISO-3 + bandera para el choropleth y la lista.
_ISO = {
    "CO": ("COL", "🇨🇴"), "MX": ("MEX", "🇲🇽"), "AR": ("ARG", "🇦🇷"),
    "CL": ("CHL", "🇨🇱"), "PE": ("PER", "🇵🇪"), "US": ("USA", "🇺🇸"),
    "ES": ("ESP", "🇪🇸"), "BR": ("BRA", "🇧🇷"), "EC": ("ECU", "🇪🇨"),
    "UY": ("URY", "🇺🇾"), "BO": ("BOL", "🇧🇴"), "PY": ("PRY", "🇵🇾"),
    "VE": ("VEN", "🇻🇪"), "CR": ("CRI", "🇨🇷"), "PA": ("PAN", "🇵🇦"),
    "DO": ("DOM", "🇩🇴"),
}


def _apply_template(fig):
    return fig.update_layout(template=PLOTLY_TEMPLATE)


def render_customer_analysis(
    customer_360: pd.DataFrame,
    filters: dict,
    source_label: str | None = None,
) -> None:
    """Renderiza la página 'Clientes'."""
    st.header("Análisis de Clientes")
    if source_label:
        st.caption(f"Fuente de datos: `{source_label}`")

    if customer_360.empty:
        st.warning(
            "El modelo `customer_360` aún no está publicado en la capa Gold. "
            "Se habilitará cuando Esteban complete US-07."
        )
        return

    selected_countries = filters.get("countries")
    filtered = filter_by_selections(customer_360, countries=selected_countries)
    if filtered.empty:
        st.info("No hay clientes en los países seleccionados. Ajusta el filtro en el sidebar.")
        return

    # --- KPIs ---------------------------------------------------------------
    total_customers = int(filtered["customer_id"].nunique()) if "customer_id" in filtered.columns else 0
    avg_ltv = float(filtered["lifetime_value"].mean()) if "lifetime_value" in filtered.columns else 0.0
    avg_orders = float(filtered["total_orders"].mean()) if "total_orders" in filtered.columns else 0.0
    render_kpi_cards(
        {"total_customers": total_customers, "avg_ltv": avg_ltv, "avg_orders": avg_orders},
        columns=["total_customers", "avg_ltv", "avg_orders"],
    )

    st.markdown("---")

    # --- Clients geography (bandera + % + barra gradiente) ------------------
    if "country" in filtered.columns:
        st.subheader("Clients geography")
        counts = filtered["country"].value_counts()
        total = int(counts.sum())
        max_count = int(counts.max()) if len(counts) else 1
        for country, n in counts.items():
            iso3, flag = _ISO.get(country, (country, ""))
            pct_of_max = (n / max_count) * 100  # barra relativa al líder
            share = (n / total * 100) if total else 0
            st.markdown(
                progress_bar_html(
                    label=f"{flag}  {country}",
                    pct=pct_of_max,
                    trailing=f"{share:.1f}%",
                ),
                unsafe_allow_html=True,
            )

        st.markdown("---")
        # Coroplético como complemento visual.
        geo = counts.reset_index()
        geo.columns = ["country", "clientes"]
        geo["iso3"] = geo["country"].map(lambda c: _ISO.get(c, (c, ""))[0])
        fig_map = px.choropleth(
            geo, locations="iso3", locationmode="ISO-3", color="clientes",
            hover_name="country", title="Distribución de clientes",
            color_continuous_scale="YlGnBu",
        )
        _apply_template(fig_map)
        st.plotly_chart(fig_map, width="stretch", config=PLOTLY_CONFIG, key="cust_map")

    # --- Segmentación LTV -----------------------------------------------------
    segments = compute_customer_segments(filtered)
    if not segments.empty:
        st.subheader("Segmentación por Lifetime Value (LTV)")
        order = [s for s in ["Bronce", "Plata", "Oro", "Premium"] if s in set(segments["segment"])]
        seg = segments["segment"].value_counts().reindex(order).fillna(0).reset_index()
        seg.columns = ["Segmento", "Clientes"]
        fig_seg = px.bar(
            seg, x="Segmento", y="Clientes", color="Segmento",
            title="Clientes por segmento de LTV",
            color_discrete_sequence=PALETTE,
        )
        _apply_template(fig_seg)
        st.plotly_chart(fig_seg, width="stretch", config=PLOTLY_CONFIG, key="seg_chart")

    # --- Tabla + descarga -------------------------------------------------------
    st.subheader("Detalle de Clientes")
    render_download_button(filtered, "Descargar clientes (CSV)", "customer_360", key="dl_customers")
    st.dataframe(filtered, width="stretch", hide_index=True)

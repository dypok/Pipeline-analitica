"""Pestaña 'Análisis de Clientes' (US-14).

Muestra:
    - Distribución de clientes por país (cuando el modelo Gold exponga la dimensión).
    - Frecuencia de compra (orders por cliente).
    - Segmentación por LTV (cuartiles: Bronce, Plata, Oro, Premium).

Filtra por ``filters["countries"]`` (origen: ``customer_360.country``).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.kpi_calculator import compute_customer_segments, filter_by_selections

# Mapping ISO-2 -> ISO-3 para el choropleth de Plotly (``locationmode='ISO-3'``).
# Los códigos ISO-2 (CO, MX, AR...) no son reconocidos directamente por Plotly.
_ISO2_TO_ISO3 = {
    "CO": "COL", "MX": "MEX", "AR": "ARG", "CL": "CHL",
    "PE": "PER", "US": "USA", "ES": "ESP", "BR": "BRA",
    "EC": "ECU", "UY": "URY", "BO": "BOL", "PY": "PRY",
    "VE": "VEN", "CR": "CRI", "PA": "PAN", "DO": "DOM",
}


def render_customer_analysis(
    customer_360: pd.DataFrame,
    filters: dict,
    source_label: str | None = None,
) -> None:
    """Renderiza la pestaña 'Análisis de Clientes'."""
    st.header("👥 Análisis de Clientes")
    if source_label:
        st.caption(f"Fuente de datos: `{source_label}`")

    if customer_360.empty:
        st.warning(
            "El modelo `customer_360` aún no está publicado en la capa Gold. "
            "Esta pestaña se habilitará cuando Esteban complete US-07 (jobs de "
            "Spark para `customer_360.parquet`)."
        )
        st.info(
            "**Contrato esperado** (ver ``docs/contratos_integracion.md``):\\n"
            "customer_id, first_purchase_date, last_purchase_date, total_orders, "
            "lifetime_value, avg_order_value, favorite_category, country"
        )
        return

    # --- Filtro de país (afecta a toda la pestaña) -------------------------
    selected_countries = filters.get("countries")
    filtered = filter_by_selections(customer_360, countries=selected_countries)

    if selected_countries:  # lista vacía => sin filtro (ver filter_by_selections)
        st.caption(
            f"Filtro activo: {len(selected_countries)} país(es) seleccionado(s) "
            f"de {customer_360['country'].nunique() if 'country' in customer_360.columns else '?'} "
            f"disponibles."
        )

    if filtered.empty:
        st.info(
            "No hay clientes en los países seleccionados. Ajusta el filtro "
            "'Países' en el sidebar para ampliar la selección."
        )
        return

    # --- Métricas resumidas (sobre el dataset filtrado) --------------------
    total_customers = int(filtered["customer_id"].nunique()) if "customer_id" in filtered.columns else 0
    avg_ltv = float(filtered["lifetime_value"].mean()) if "lifetime_value" in filtered.columns else 0.0
    avg_orders = float(filtered["total_orders"].mean()) if "total_orders" in filtered.columns else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Clientes Totales", f"{total_customers:,}")
    c2.metric("LTV Promedio", f"${avg_ltv:,.2f}")
    c3.metric("Órdenes Promedio/Cliente", f"{avg_orders:.2f}")

    st.markdown("---")

    # --- Segmentación por LTV ---------------------------------------------
    segments = compute_customer_segments(filtered)
    if not segments.empty:
        st.subheader("Segmentación por Lifetime Value (LTV)")
        seg_counts = segments["segment"].value_counts().reset_index()
        seg_counts.columns = ["Segmento", "Clientes"]
        fig_seg = px.bar(
            seg_counts,
            x="Segmento",
            y="Clientes",
            color="Segmento",
            title="Distribución de Clientes por Segmento de LTV",
            template="plotly_white",
        )
        st.plotly_chart(fig_seg, width="stretch")

    # --- Distribución por país --------------------------------------------
    if "country" in filtered.columns:
        country_df = filtered["country"].value_counts().reset_index()
        country_df.columns = ["País", "Clientes"]
        # Plotly choropleth requiere ISO-3; mapear desde ISO-2 si aplica.
        country_df["iso3"] = country_df["País"].map(_ISO2_TO_ISO3).fillna(country_df["País"])
        title = "Distribución de Clientes por País"
        if selected_countries:
            title += f" ({len(selected_countries)} país(es) seleccionado(s))"
        fig_country = px.choropleth(
            country_df,
            locations="iso3",
            locationmode="ISO-3",
            color="Clientes",
            hover_name="País",
            hover_data={"iso3": False, "Clientes": True},
            title=title,
            template="plotly_white",
            color_continuous_scale="Blues",
        )
        st.plotly_chart(fig_country, width="stretch")
    else:
        st.info(
            "La distribución por país se habilitará cuando el modelo Gold "
            "incluya la dimensión geográfica (US-07)."
        )

    # --- Tabla interactiva -------------------------------------------------
    st.subheader("Detalle de Clientes")
    st.dataframe(filtered, width="stretch", hide_index=True)

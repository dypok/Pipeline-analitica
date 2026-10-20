"""Pestaña 'Análisis de Clientes' (US-14).

Muestra:
    - Distribución de clientes por país (cuando el modelo Gold exponga la dimensión).
    - Frecuencia de compra (orders por cliente).
    - Segmentación por LTV (cuartiles: Bronce, Plata, Oro, Premium).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.kpi_calculator import compute_customer_segments


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
            "lifetime_value, avg_order_value, favorite_category"
        )
        return

    # --- Métricas resumidas -----------------------------------------------
    total_customers = int(customer_360["customer_id"].nunique()) if "customer_id" in customer_360.columns else 0
    avg_ltv = float(customer_360["lifetime_value"].mean()) if "lifetime_value" in customer_360.columns else 0.0
    avg_orders = float(customer_360["total_orders"].mean()) if "total_orders" in customer_360.columns else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Clientes Totales", f"{total_customers:,}")
    c2.metric("LTV Promedio", f"${avg_ltv:,.2f}")
    c3.metric("Órdenes Promedio/Cliente", f"{avg_orders:.2f}")

    st.markdown("---")

    # --- Segmentación por LTV ---------------------------------------------
    segments = compute_customer_segments(customer_360)
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

    # --- Distribución por país (pendiente) --------------------------------
    if "country" in customer_360.columns:
        country_df = customer_360["country"].value_counts().reset_index()
        country_df.columns = ["País", "Clientes"]
        fig_country = px.choropleth(
            country_df,
            locations="País",
            locationmode="country names",
            color="Clientes",
            title="Distribución de Clientes por País",
            template="plotly_white",
        )
        st.plotly_chart(fig_country, width="stretch")
    else:
        st.info(
            "La distribución por país se habilitará cuando el modelo Gold "
            "incluya la dimensión geográfica (US-07)."
        )

    # --- Tabla interactiva -------------------------------------------------
    st.subheader("Detalle de Clientes")
    st.dataframe(customer_360, width="stretch", hide_index=True)

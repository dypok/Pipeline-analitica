"""Pestaña 'Rendimiento de Productos' (US-14).

Muestra:
    - Tabla interactiva con búsqueda de productos más vendidos y márgenes.
    - Ranking de productos por ingresos brutos.
    - Tasa de devolución por producto (return_rate).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st


def render_product_performance(
    product_performance: pd.DataFrame,
    filters: dict,
    source_label: str | None = None,
) -> None:
    """Renderiza la pestaña 'Rendimiento de Productos'."""
    st.header("📦 Rendimiento de Productos")
    if source_label:
        st.caption(f"Fuente de datos: `{source_label}`")

    if product_performance.empty:
        st.warning(
            "El modelo `product_performance` aún no está publicado en la capa Gold. "
            "Esta pestaña se habilitará cuando Esteban complete US-07."
        )
        st.info(
            "**Contrato esperado** (ver ``docs/contratos_integracion.md``):\\n"
            "product_id, units_sold, gross_revenue, return_rate, margin"
        )
        return

    # --- KPIs agregados de productos --------------------------------------
    total_products = int(product_performance["product_id"].nunique()) if "product_id" in product_performance.columns else 0
    total_units = int(product_performance["units_sold"].sum()) if "units_sold" in product_performance.columns else 0
    total_gross = float(product_performance["gross_revenue"].sum()) if "gross_revenue" in product_performance.columns else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Productos Analizados", f"{total_products:,}")
    c2.metric("Unidades Vendidas", f"{total_units:,}")
    c3.metric("Ingresos Brutos", f"${total_gross:,.2f}")

    st.markdown("---")

    # --- Búsqueda interactiva ---------------------------------------------
    search = st.text_input("🔎 Buscar producto por ID", placeholder="Ej. PROD-0001")
    df = product_performance.copy()
    if search and "product_id" in df.columns:
        df = df[df["product_id"].astype(str).str.contains(search, case=False, na=False)]

    # --- Top productos por ingreso bruto ----------------------------------
    if "gross_revenue" in df.columns:
        top_df = df.nlargest(15, "gross_revenue")
        fig = px.bar(
            top_df,
            x="gross_revenue",
            y="product_id",
            orientation="h",
            title="Top 15 Productos por Ingreso Bruto",
            labels={"gross_revenue": "Ingreso bruto (USD)", "product_id": "Producto"},
            template="plotly_white",
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, width="stretch")

    # --- Tabla interactiva -------------------------------------------------
    st.subheader("Detalle por Producto")
    st.dataframe(df, width="stretch", hide_index=True)

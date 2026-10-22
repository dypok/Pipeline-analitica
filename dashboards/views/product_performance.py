"""Pestaña 'Rendimiento de Productos' (US-14).

Muestra:
    - Métricas clave de unidades vendidas, ingresos brutos y tasa de devolución.
    - Tabla interactiva con búsqueda de productos por ID o Nombre y márgenes.
    - Ranking de productos por ingresos brutos.
    - Tasa de devolución por producto (return_rate y returns_count).
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
            "**Contrato esperado** (ver ``docs/contratos_integracion.md``):\n"
            "product_id, product_name, units_sold, gross_revenue, returns_count, return_rate, margin"
        )
        return

    # --- KPIs agregados de productos --------------------------------------
    total_products = int(product_performance["product_id"].nunique()) if "product_id" in product_performance.columns else 0
    total_units = int(product_performance["units_sold"].sum()) if "units_sold" in product_performance.columns else 0
    total_gross = float(product_performance["gross_revenue"].sum()) if "gross_revenue" in product_performance.columns else 0.0
    total_returns = int(product_performance["returns_count"].sum()) if "returns_count" in product_performance.columns else 0
    avg_return_rate = (
        float(total_returns / total_units * 100) if total_units > 0
        else (float(product_performance["return_rate"].mean() * 100) if "return_rate" in product_performance.columns else 0.0)
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Productos Analizados", f"{total_products:,}")
    c2.metric("Unidades Vendidas", f"{total_units:,}")
    c3.metric("Ingresos Brutos", f"${total_gross:,.2f}")
    c4.metric("Total Devoluciones", f"{total_returns:,}")
    c5.metric("Tasa Devolución", f"{avg_return_rate:.2f}%")

    st.markdown("---")

    # --- Búsqueda interactiva ---------------------------------------------
    search = st.text_input("🔎 Buscar producto por ID o Nombre", placeholder="Ej. PROD-0001 o Smartwatch")
    df = product_performance.copy()
    if search:
        m_id = df["product_id"].astype(str).str.contains(search, case=False, na=False) if "product_id" in df.columns else False
        m_name = df["product_name"].astype(str).str.contains(search, case=False, na=False) if "product_name" in df.columns else False
        df = df[m_id | m_name]

    # --- Top productos por ingreso bruto ----------------------------------
    if "gross_revenue" in df.columns and not df.empty:
        top_df = df.nlargest(15, "gross_revenue").copy()
        if "product_name" in top_df.columns:
            top_df["display_label"] = top_df["product_id"].astype(str) + " - " + top_df["product_name"].astype(str)
        else:
            top_df["display_label"] = top_df["product_id"].astype(str)

        fig = px.bar(
            top_df,
            x="gross_revenue",
            y="display_label",
            orientation="h",
            title="Top 15 Productos por Ingreso Bruto",
            labels={"gross_revenue": "Ingreso bruto (USD)", "display_label": "Producto"},
            template="plotly_white",
            color="category" if "category" in top_df.columns else None,
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"})
        st.plotly_chart(fig, width="stretch")

    # --- Tabla interactiva -------------------------------------------------
    st.subheader("Detalle por Producto")
    cols_order = [
        c for c in [
            "product_id", "product_name", "category", "units_sold",
            "gross_revenue", "returns_count", "return_rate", "margin", "performance_tier"
        ] if c in df.columns
    ]
    other_cols = [c for c in df.columns if c not in cols_order and not c.startswith("_")]
    df_display = df[cols_order + other_cols].copy()

    column_config = {
        "product_id": st.column_config.TextColumn("ID Producto", help="Identificador único del producto"),
        "product_name": st.column_config.TextColumn("Nombre Producto"),
        "category": st.column_config.TextColumn("Categoría"),
        "units_sold": st.column_config.NumberColumn("Unidades Vendidas", format="%d"),
        "gross_revenue": st.column_config.NumberColumn("Ingreso Bruto", format="$%.2f"),
        "returns_count": st.column_config.NumberColumn("Devoluciones", format="%d"),
        "return_rate": st.column_config.NumberColumn("Tasa Devolución", format="%.2f%%"),
        "margin": st.column_config.NumberColumn("Margen", format="$%.2f"),
        "performance_tier": st.column_config.TextColumn("Nivel de Desempeño"),
    }

    if "return_rate" in df_display.columns:
        if (df_display["return_rate"].dropna() <= 1.0).all():
            df_display["return_rate"] = df_display["return_rate"] * 100.0

    st.dataframe(df_display, width="stretch", hide_index=True, column_config=column_config)

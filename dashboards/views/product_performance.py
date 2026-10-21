"""Pestaña 'Rendimiento de Productos' (US-14) — tema Oscuro Premium.

Muestra:
    - Métricas premium (Productos, Unidades, Ingresos Brutos).
    - Ranking de productos por ingreso bruto (barras horizontales).
    - Búsqueda interactiva por ID o nombre.
    - Tabla detallada + descarga del filtrado.

Filtra por ``filters["categories"]`` (origen: ``product_performance.category``).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.components.download import render_download_button
from dashboards.components.kpi_cards import render_kpi_cards
from dashboards.kpi_calculator import filter_by_selections
from dashboards.styles import PLOTLY_CONFIG, PLOTLY_TEMPLATE

# Escala cian para el coloreado por ingreso (estilo observabilidad).
REVENUE_SCALE = ["#0A2233", "#10455C", "#1B92A8", "#22D3EE", "#7FF0FF"]


def _apply_template(fig):
    return fig.update_layout(template=PLOTLY_TEMPLATE)


def render_product_performance(
    product_performance: pd.DataFrame,
    filters: dict,
    source_label: str | None = None,
) -> None:
    """Renderiza la página 'Productos'."""
    st.header("Rendimiento de Productos")
    if source_label:
        st.caption(f"Fuente de datos: `{source_label}`")

    if product_performance.empty:
        st.warning(
            "El modelo `product_performance` aún no está publicado en la capa "
            "Gold. Se habilitará cuando Esteban complete US-07."
        )
        st.info(
            "**Contrato esperado** (``docs/contratos_integracion.md``): "
            "product_id, units_sold, gross_revenue, return_rate, margin"
        )
        return

    selected_categories = filters.get("categories")
    filtered = filter_by_selections(product_performance, categories=selected_categories)

    if filtered.empty:
        st.info("No hay productos en las categorías seleccionadas. Ajusta el filtro en el sidebar.")
        return

    # --- Métricas premium ---------------------------------------------------
    total_products = int(filtered["product_id"].nunique()) if "product_id" in filtered.columns else 0
    total_units = int(filtered["units_sold"].sum()) if "units_sold" in filtered.columns else 0
    total_gross = float(filtered["gross_revenue"].sum()) if "gross_revenue" in filtered.columns else 0.0
    render_kpi_cards(
        {"total_products": total_products, "total_units": total_units, "total_gross": total_gross},
        columns=["total_products", "total_units", "total_gross"],
    )

    st.markdown("---")

    # --- Búsqueda interactiva -----------------------------------------------
    search = st.text_input("Buscar producto", placeholder="Ej. PROD-0001 o 'Smartphone'")
    df = filtered.copy()
    if search:
        mask = pd.Series(False, index=df.index)
        if "product_id" in df.columns:
            mask |= df["product_id"].astype(str).str.contains(search, case=False, na=False)
        if "product_name" in df.columns:
            mask |= df["product_name"].astype(str).str.contains(search, case=False, na=False)
        df = df[mask]

    # --- Top productos por ingreso bruto -------------------------------------
    if "gross_revenue" in df.columns and not df.empty:
        top_n = min(15, len(df))
        top_df = df.nlargest(top_n, "gross_revenue")
        y_col = "product_name" if "product_name" in top_df.columns else "product_id"
        title = "Top Productos por Ingreso Bruto"
        if selected_categories:
            title += f" ({len(selected_categories)} categoría(s))"
        fig = px.bar(
            top_df,
            x="gross_revenue",
            y=y_col,
            orientation="h",
            title=title,
            labels={"gross_revenue": "Ingreso bruto (USD)", y_col: "Producto"},
            color="gross_revenue",
            color_continuous_scale=REVENUE_SCALE,
        )
        fig.update_layout(yaxis={"categoryorder": "total ascending"}, coloraxis_showscale=False)
        _apply_template(fig)
        st.plotly_chart(fig, width="stretch", config=PLOTLY_CONFIG, key="prod_chart")

    # --- Tabla + descarga -----------------------------------------------------
    st.subheader("Detalle por Producto")
    if df.empty:
        st.info("La búsqueda no arrojó resultados.")
    else:
        render_download_button(df, "Descargar productos (CSV)", "product_performance", key="dl_products")
        st.dataframe(df, width="stretch", hide_index=True)

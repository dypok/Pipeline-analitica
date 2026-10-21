"""Pestaña 'Rendimiento de Productos' (US-14).

Muestra:
    - Tabla interactiva con búsqueda de productos más vendidos y márgenes.
    - Ranking de productos por ingresos brutos.
    - Tasa de devolución por producto (return_rate).

Filtra por ``filters["categories"]`` (origen: ``product_performance.category``).
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st

from dashboards.kpi_calculator import filter_by_selections


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

    # --- Filtro de categoría (afecta a toda la pestaña) -------------------
    selected_categories = filters.get("categories")
    filtered = filter_by_selections(product_performance, categories=selected_categories)

    if selected_categories and "category" in product_performance.columns:
        st.caption(
            f"Filtro activo: {len(selected_categories)} categoría(s) seleccionada(s) "
            f"de {product_performance['category'].nunique()} disponibles."
        )

    if filtered.empty:
        st.info(
            "No hay productos en las categorías seleccionadas. Ajusta el filtro "
            "'Categorías' en el sidebar para ampliar la selección."
        )
        return

    # --- KPIs agregados de productos (sobre el dataset filtrado) -----------
    total_products = int(filtered["product_id"].nunique()) if "product_id" in filtered.columns else 0
    total_units = int(filtered["units_sold"].sum()) if "units_sold" in filtered.columns else 0
    total_gross = float(filtered["gross_revenue"].sum()) if "gross_revenue" in filtered.columns else 0.0

    c1, c2, c3 = st.columns(3)
    c1.metric("Productos Analizados", f"{total_products:,}")
    c2.metric("Unidades Vendidas", f"{total_units:,}")
    c3.metric("Ingresos Brutos", f"${total_gross:,.2f}")

    st.markdown("---")

    # --- Búsqueda interactiva ---------------------------------------------
    # Busca por product_id Y product_name si ambos están disponibles.
    search = st.text_input("🔎 Buscar producto", placeholder="Ej. PROD-0001 o 'Smartphone'")
    df = filtered.copy()
    if search:
        mask = pd.Series(False, index=df.index)
        if "product_id" in df.columns:
            mask |= df["product_id"].astype(str).str.contains(search, case=False, na=False)
        if "product_name" in df.columns:
            mask |= df["product_name"].astype(str).str.contains(search, case=False, na=False)
        df = df[mask]

    # --- Top productos por ingreso bruto ----------------------------------
    if "gross_revenue" in df.columns:
        top_n = min(15, len(df))
        if top_n > 0:
            top_df = df.nlargest(top_n, "gross_revenue")
            # Etiqueta del eje Y: usar product_name si existe, si no product_id.
            y_col = "product_name" if "product_name" in top_df.columns else "product_id"
            title = "Top Productos por Ingreso Bruto"
            if selected_categories and "category" in product_performance.columns:
                title += f" ({len(selected_categories)} categoría(s))"
            fig = px.bar(
                top_df,
                x="gross_revenue",
                y=y_col,
                orientation="h",
                title=title,
                labels={
                    "gross_revenue": "Ingreso bruto (USD)",
                    y_col: "Producto",
                },
                template="plotly_white",
            )
            fig.update_layout(yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, width="stretch")

    # --- Tabla interactiva -------------------------------------------------
    st.subheader("Detalle por Producto")
    if df.empty:
        st.info("La búsqueda no arrojó resultados.")
    else:
        st.dataframe(df, width="stretch", hide_index=True)

"""Componente de descarga de datasets filtrados (CSV).

Permite descargar el DataFrame filtrado de cada pestaña directamente desde la
UI, con nombre de archivo descriptivo y fecha. Cumple la mejora de
interactividad "Descarga CSV por pestaña".
"""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st


def render_download_button(
    df: pd.DataFrame,
    label: str,
    filename_prefix: str,
    key: str | None = None,
) -> None:
    """Renderiza un botón de descarga CSV del DataFrame ``df``.

    Args:
        df: DataFrame a exportar (ya filtrado por la pestaña).
        label: Texto del botón (p. ej. "Descargar ventas filtradas (CSV)").
        filename_prefix: Prefijo del archivo (p. ej. "daily_sales").
        key: Clave única de Streamlit (necesaria si hay varios botones).
    """
    if df is None or df.empty:
        return
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button(
        data=csv,
        file_name=f"{filename_prefix}_{stamp}.csv",
        mime="text/csv",
        label=label,
        key=key or f"dl_{filename_prefix}",
    )

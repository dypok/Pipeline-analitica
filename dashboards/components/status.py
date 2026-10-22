"""Componente de estado: refresco y última actualización (US-14).

Muestra en el sidebar la última vez que se cargaron los datos y un botón para
forzar la recarga (invalida la caché de ``st.cache_data``). Cumple la mejora
de interactividad "Refresco y estado de carga".
"""

from __future__ import annotations

from datetime import datetime

import streamlit as st


def render_refresh_control(load_fn) -> None:
    """Renderiza el bloque de refresco en el sidebar.

    Args:
        load_fn: Función cacheada de carga de datos (``st.cache_data``). Al
            pulsar "Refresher" se invalida su caché y se fuerza un rerun.
    """
    stamp = st.session_state.get("last_loaded_at")
    st.sidebar.markdown("---")
    col1, col2 = st.sidebar.columns([3, 1])
    with col1:
        st.caption(f"Datos: {stamp or '—'}")
    with col2:
        if st.button("Refresh", key="btn_refresh", help="Recargar datos desde la fuente", use_container_width=True):
            load_fn.clear()
            st.session_state["last_loaded_at"] = datetime.now().strftime("%H:%M:%S")
            st.rerun()


def mark_loaded() -> None:
    """Registra el timestamp de la última carga (llamar tras cargar datos)."""
    st.session_state["last_loaded_at"] = datetime.now().strftime("%H:%M:%S")

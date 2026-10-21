"""Indicadores de estado de servicios (sidebar) — DataOps Monitor.

Comprueba la conectividad real (socket) a los servicios de la plataforma y
pinta un indicador por servicio en el sidebar:

    - PostgreSQL (analytics_db)  -> puerto 5433
    - MinIO (object storage S3)  -> puerto 9000
    - Spark Master UI             -> puerto 8081
    - Airflow Web UI              -> puerto 8080

NOTA: el stack es un lakehouse **batch** Medallion; no incluye Kafka (eso sería
un pipeline de streaming). Se listan los servicios reales del proyecto.
"""

from __future__ import annotations

import socket

import streamlit as st

from dashboards.styles import service_indicator_html

# (nombre, etiqueta corta, host, puerto)
_SERVICES = [
    ("PostgreSQL", "PG", "localhost", 5433),
    ("MinIO", "S3", "localhost", 9000),
    ("Spark", "SPK", "localhost", 8081),
    ("Airflow", "AF", "localhost", 8080),
]


def _reachable(host: str, port: int, timeout: float = 0.35) -> bool:
    """¿Está el servicio escuchando en el puerto?"""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


@st.cache_data(ttl=30, show_spinner=False)
def check_services() -> dict[str, bool]:
    """Cachea el chequeo de conectividad (30s) para no latir en cada rerun."""
    return {name: _reachable(host, port) for name, _icon, host, port in _SERVICES}


def render_service_indicators() -> None:
    """Pinta el bloque de indicadores de servicio en el sidebar."""
    st.sidebar.markdown('<div class="menu-title">Servicios</div>', unsafe_allow_html=True)
    states = check_services()
    up_count = sum(states.values())
    html = ""
    for name, icon, _host, _port in _SERVICES:
        html += service_indicator_html(name, icon, states.get(name, False))
    st.sidebar.markdown(html, unsafe_allow_html=True)
    st.sidebar.caption(f"{up_count}/{len(_SERVICES)} servicios activos")

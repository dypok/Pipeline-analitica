"""Vistas (pestañas) del dashboard Streamlit.

Cada vista corresponde a una pestaña de navegación definida en US-14 y US-15:

    - ``sales_overview``       -> Visión General de Ventas
    - ``customer_analysis``    -> Análisis de Clientes
    - ``product_performance``  -> Rendimiento de Productos
    - ``chatbot_assistant``    -> Asistente IA (US-15)
    - ``pipeline_health``     -> Salud del Pipeline (US-15)
"""

from dashboards.views.sales_overview import render_sales_overview
from dashboards.views.customer_analysis import render_customer_analysis
from dashboards.views.product_performance import render_product_performance
from dashboards.views.chatbot_assistant import render_chatbot_assistant
from dashboards.views.pipeline_health import render_pipeline_health

__all__ = [
    "render_sales_overview",
    "render_customer_analysis",
    "render_product_performance",
    "render_chatbot_assistant",
    "render_pipeline_health",
]

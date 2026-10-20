"""Módulo de Dashboards y Capa de Presentación (Sergio).

Aplicación Streamlit que consume los modelos analíticos de la capa Gold
(`daily_sales`, `customer_360`, `product_performance`) y los expone mediante
visualizaciones interactivas, filtros dinámicos y un asistente conversacional.

Historias de usuario cubiertas:
    - US-14: Dashboard Interactivo de Negocio (Retail KPIs y Filtros) en Streamlit.
    - US-15: Interfaz Integrada para el Chatbot Conversacional y Monitor de Calidad.

Punto de entrada:
    ``streamlit run dashboards/main.py``
"""

__all__ = ["main"]

"""Pestaña 'Asistente IA' (US-15).

Integra el chatbot conversacional de Dylan en la UI de Streamlit usando
``st.chat_message`` y ``st.chat_input``. Conecta con el endpoint
``POST /api/v1/chatbot/ask`` del backend FastAPI.

Pruebas de aceptación:
    - P15.1: el historial de mensajes se conserva durante la sesión.
    - P09 (anti-alucinación): el chatbot responde con la cifra real respaldada
      por SQL, y explica la limitación ante preguntas sin datos.
"""

from __future__ import annotations

import pandas as pd
import streamlit as st

from dashboards.chatbot_client import ChatbotClient, get_client  # type: ignore[attr-defined]


def render_chatbot_assistant() -> None:
    """Renderiza la pestaña 'Asistente IA'."""
    st.header("Asistente IA")
    st.markdown(
        "Haz preguntas en lenguaje natural sobre las ventas, clientes y "
        "productos. El asistente traduce tu pregunta a SQL sobre la capa Gold "
        "y responde con la cifra real (sin alucinar)."
    )

    client = get_client()

    # Estado de disponibilidad del backend
    backend_ok = client.health()
    if not backend_ok:
        st.warning(
            f"No se pudo conectar al backend del chatbot en `{client.base_url}`. "
            "Asegúrate de que Dylan tenga levantado el servicio FastAPI (US-13). "
            "Podrás seguir escribiendo mensajes pero recibirás un mensaje de error."
        )

    # --- Inicializar historial de sesión ---------------------------------
    if "chat_history" not in st.session_state:
        st.session_state.chat_history = [
            {
                "role": "assistant",
                "content": (
                    "¡Hola! Soy el asistente analítico. Puedes preguntarme cosas "
                    "como *'¿Cuál fue el ingreso total de ayer?'* o "
                    "*'¿Cuántas órdenes completamos en octubre?'*."
                ),
            }
        ]

    # --- Renderizar historial --------------------------------------------
    for message in st.session_state.chat_history:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
            # Si la respuesta incluye datos tabulares, renderizarlos
            if "rows" in message and message["rows"]:
                df = pd.DataFrame(message["rows"], columns=message.get("columns") or None)
                st.dataframe(df, width="stretch", hide_index=True)
            if "sql" in message and message["sql"]:
                with st.expander("Ver SQL generado"):
                    st.code(message["sql"], language="sql")

    # --- Input del usuario ----------------------------------------------
    question = st.chat_input("Escribe tu pregunta sobre los datos...")
    if not question:
        return

    # Registrar la pregunta del usuario
    st.session_state.chat_history.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.markdown(question)

    # Consultar al backend
    with st.chat_message("assistant"):
        with st.spinner("Consultando la capa Gold..."):
            answer = client.ask(question)
        if answer.backend_reachable and not answer.error and answer.answer:
            st.markdown(answer.answer)
            if answer.rows:
                df = pd.DataFrame(answer.rows, columns=answer.columns or None)
                st.dataframe(df, width="stretch", hide_index=True)
            if answer.sql:
                with st.expander("Ver SQL generado"):
                    st.code(answer.sql, language="sql")
            st.session_state.chat_history.append(
                {
                    "role": "assistant",
                    "content": answer.answer,
                    "rows": answer.rows,
                    "columns": answer.columns,
                    "sql": answer.sql,
                }
            )
        else:
            error_msg = answer.error or answer.answer or "Error desconocido al procesar la solicitud."
            st.error(f"⚠️ {error_msg}")
            st.session_state.chat_history.append(
                {"role": "assistant", "content": f"⚠️ {error_msg}"}
            )

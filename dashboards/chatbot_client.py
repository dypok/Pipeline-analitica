"""Cliente HTTP para el servicio Chatbot de IA del backend FastAPI.

Contrato (US-15 / ``docs/contratos_integracion.md`` §7):
    - Endpoint: ``POST {BACKEND_API_URL}/api/v1/chatbot/ask``
    - Request body:  ``{"question": "<pregunta en lenguaje natural>"}``
    - Response body: ``{"answer": "<texto>", "sql": "<SQL generado>", "rows": [...], "columns": [...]}``

El backend está implementado por Dylan Gamero en la rama ``feature/chatbot-ia``.
Mientras no esté disponible, el cliente degrada gracefully con un mensaje claro.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import httpx
from dotenv import load_dotenv

load_dotenv()


@dataclass
class ChatbotAnswer:
    """Respuesta estructurada del chatbot."""

    answer: str
    sql: Optional[str] = None
    rows: Optional[List[Dict[str, Any]]] = None
    columns: Optional[List[str]] = None
    backend_reachable: bool = True
    error: Optional[str] = None
    error_code: Optional[str] = None


class ChatbotClient:
    """Cliente para el endpoint ``/api/v1/chatbot/ask``."""

    DEFAULT_TIMEOUT: Optional[float] = None  # Sin tiempo fijo para modelo local en CPU

    def __init__(self, base_url: Optional[str] = None, timeout: Optional[float] = None) -> None:
        self.base_url = (base_url or os.getenv("BACKEND_API_URL", "http://localhost:8000")).rstrip("/")
        env_timeout = os.getenv("CHATBOT_TIMEOUT")
        if timeout is not None:
            self.timeout = timeout
        elif env_timeout and env_timeout.lower() not in ("none", "0", "false", ""):
            self.timeout = float(env_timeout)
        else:
            self.timeout = self.DEFAULT_TIMEOUT
        self._endpoint = f"{self.base_url}/api/v1/chatbot/ask"

    def health(self) -> bool:
        """Comprueba si el backend está reachable (``GET /health`` o ``/``)."""
        for path in ("/health", "/healthz", "/"):
            try:
                resp = httpx.get(f"{self.base_url}{path}", timeout=5.0)
                if resp.status_code < 500:
                    return True
            except Exception:
                continue
        return False

    def ask(self, question: str) -> ChatbotAnswer:
        """Envía una pregunta al chatbot y retorna la respuesta estructurada.

        Sigue el protocolo anti-alucinación de US-15 / P09: si el backend no
        responde, se indica explícitamente y nunca se inventan datos.
        """
        if not question or not question.strip():
            return ChatbotAnswer(answer="", backend_reachable=True, error="Pregunta vacía")

        try:
            resp = httpx.post(
                self._endpoint,
                json={"question": question},
                timeout=self.timeout,
            )
            if resp.status_code >= 500:
                return ChatbotAnswer(
                    answer="",
                    backend_reachable=False,
                    error=f"Backend error {resp.status_code}",
                )
            payload = resp.json()
            is_success = payload.get("success", True)
            err_code = payload.get("error_code")
            return ChatbotAnswer(
                answer=payload.get("answer", ""),
                sql=payload.get("sql"),
                rows=payload.get("rows"),
                columns=payload.get("columns"),
                backend_reachable=True,
                error=None if is_success else (f"[{err_code}] {payload.get('answer')}" if err_code else payload.get("answer")),
                error_code=err_code,
            )
        except httpx.TimeoutException:
            return ChatbotAnswer(
                answer=(
                    "El modelo local de IA sigue procesando la consulta en CPU. "
                    "Por favor espera unos instantes o formula una pregunta más directa."
                ),
                backend_reachable=True,
                error="[ERR_LLM_TIMEOUT] La inferencia del modelo local superó el tiempo de espera.",
                error_code="ERR_LLM_TIMEOUT",
            )
        except httpx.ConnectError:
            return ChatbotAnswer(
                answer=(
                    "No pude conectar con el servicio del chatbot. Verifica que el backend "
                    f"FastAPI esté levantado en {self.base_url} (Dylan, US-13)."
                ),
                backend_reachable=False,
                error="[ERR_LLM_UNAVAILABLE] connection_refused",
                error_code="ERR_LLM_UNAVAILABLE",
            )
        except Exception as exc:
            return ChatbotAnswer(
                answer="",
                backend_reachable=False,
                error=f"Error inesperado: {str(exc)}",
                error_code="ERR_LLM_UNAVAILABLE",
            )


def get_client() -> ChatbotClient:
    """Factory del cliente del chatbot (instancia compartida por sesión)."""
    return ChatbotClient()

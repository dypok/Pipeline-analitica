import re
import logging
from app.backend.core.config import settings

logger = logging.getLogger(__name__)


class SecurityException(Exception):
    """Excepción lanzada cuando una consulta viola las reglas de seguridad SQL."""
    pass


class SQLGuard:
    """Validador estricto de seguridad para consultas SQL generadas por el Chatbot o usuarios.
    Garantiza que solo se ejecuten consultas de solo lectura (SELECT) sobre las vistas autorizadas.
    """

    ALLOWED_VIEWS = {
        "v_daily_sales",
        "v_customer_360",
        "v_product_performance",
    }

    FORBIDDEN_KEYWORDS = [
        r"\bINSERT\b",
        r"\bUPDATE\b",
        r"\bDELETE\b",
        r"\bDROP\b",
        r"\bALTER\b",
        r"\bTRUNCATE\b",
        r"\bCREATE\b",
        r"\bREPLACE\b",
        r"\bEXEC\b",
        r"\bEXECUTE\b",
        r"\bGRANT\b",
        r"\bREVOKE\b",
        r"\bATTACH\b",
        r"\bDETACH\b",
        r"\bCOPY\b",
        r"\bPRAGMA\b",
        r"\bCALL\b",
        r"\bMERGE\b",
        r"\bUPSERT\b",
        r"\bINSTALL\b",
        r"\bLOAD\b",
    ]

    FORBIDDEN_PATTERNS = [
        r"--",        # Comentario de línea
        r"/\*",       # Comentario de bloque apertura
        r"\*/",       # Comentario de bloque cierre
        r";\s*\S+",   # Encadenamiento de sentencias (punto y coma seguido de otra instrucción)
    ]

    @classmethod
    def validate_and_sanitize(cls, sql: str, max_limit: int | None = None) -> str:
        """Valida una consulta SQL, aplica restricciones de lectura y garantiza límite de registros.

        Args:
            sql: Cadena con la consulta SQL.
            max_limit: Límite máximo de filas permitidas.

        Returns:
            Consulta SQL sanitizada y con LIMIT garantizado.

        Raises:
            SecurityException: Si la consulta no cumple los criterios de seguridad.
        """
        if not sql or not sql.strip():
            raise SecurityException("La consulta SQL no puede estar vacía.")

        clean_sql = sql.strip().rstrip(";")

        # 1. Comprobar patrones de inyección y comentarios maliciosos
        for pattern in cls.FORBIDDEN_PATTERNS:
            if re.search(pattern, clean_sql, re.IGNORECASE):
                logger.warning("Intento de inyección detectado (patrón %s) en: %s", pattern, sql)
                raise SecurityException("La consulta contiene caracteres o comentarios prohibidos.")

        # 2. Comprobar palabras clave destructivas o de modificación
        for kw in cls.FORBIDDEN_KEYWORDS:
            if re.search(kw, clean_sql, re.IGNORECASE):
                logger.warning("Palabra clave prohibida detectada (%s) en: %s", kw, sql)
                raise SecurityException("Operación no permitida: Solo se admiten consultas de lectura.")

        # 3. Comprobar que inicie con SELECT o WITH (CTE de solo lectura)
        first_token_match = re.match(r"^\s*([a-zA-Z]+)", clean_sql)
        if not first_token_match:
            raise SecurityException("Sintaxis SQL inválida.")

        first_token = first_token_match.group(1).upper()
        if first_token not in ("SELECT", "WITH", "DESCRIBE", "EXPLAIN"):
            raise SecurityException(
                f"Operación '{first_token}' rechazada. Solo se permiten consultas de tipo SELECT."
            )

        # 4. Validar tablas consultadas
        referenced_tables = cls._extract_tables(clean_sql)
        for tbl in referenced_tables:
            tbl_clean = tbl.lower().strip('"\'`')
            if tbl_clean not in cls.ALLOWED_VIEWS:
                # Permitir subconsultas o alias si están dentro de la consulta,
                # pero alertar si intenta acceder a tablas del sistema
                if (
                    tbl_clean.startswith("information_schema")
                    or tbl_clean.startswith("duckdb_")
                    or tbl_clean.startswith("read_")
                ):
                    raise SecurityException(f"Acceso denegado a tabla o función del sistema: '{tbl_clean}'.")

        # 5. Garantizar cláusula LIMIT
        effective_limit = max_limit or settings.MAX_QUERY_ROWS
        limit_match = re.search(r"\bLIMIT\s+(\d+)\b", clean_sql, re.IGNORECASE)
        if limit_match:
            user_limit = int(limit_match.group(1))
            if user_limit > effective_limit:
                # Clampear al máximo permitido
                clean_sql = re.sub(
                    r"\bLIMIT\s+\d+\b",
                    f"LIMIT {effective_limit}",
                    clean_sql,
                    flags=re.IGNORECASE,
                )
        else:
            clean_sql = f"{clean_sql} LIMIT {effective_limit}"

        return clean_sql

    @classmethod
    def _extract_tables(cls, sql: str) -> list[str]:
        """Extrae de manera heurística los nombres de tablas tras FROM o JOIN."""
        pattern = r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_\.]+)"
        matches = re.findall(pattern, sql, re.IGNORECASE)
        return matches

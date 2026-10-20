"""Utilidad de logging estructurado y estandarizado para la plataforma Medallion."""

import logging
import os
import sys
from typing import Optional


def get_logger(name: str, level: Optional[str] = None) -> logging.Logger:
    """Configura y retorna un logger estructurado con formato consistente."""
    logger = logging.getLogger(name)

    if not logger.handlers:
        log_level_str = level or os.getenv("LOG_LEVEL", "INFO").upper()
        log_level = getattr(logging, log_level_str, logging.INFO)
        logger.setLevel(log_level)

        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(log_level)

        formatter = logging.Formatter(
            fmt="%(asctime)s [%(levelname)s] [%(name)s] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False

    return logger

"""Contrato base para conectores de ingesta."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class IngestionResult:
    """Resultado estructurado de una operación de ingesta."""

    source_name: str
    destination_uri: str
    records_count: int
    bytes_written: int
    status: str  # SUCCESS, FAILED, PARTIAL
    started_at: datetime
    finished_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


class BaseConnector(ABC):
    """Clase base abstracta para conectores de ingesta hacia la capa Landing."""

    def __init__(self, source_name: str, config: Optional[Dict[str, Any]] = None):
        self.source_name = source_name
        self.config = config or {}

    @abstractmethod
    def extract(self) -> List[Dict[str, Any]]:
        """Extrae los datos desde la fuente origen (archivo, API, BD)."""
        pass

    @abstractmethod
    def ingest(self, destination_bucket: str, destination_prefix: str) -> IngestionResult:
        """Extrae y deposita los datos crudos en la capa Landing de MinIO."""
        pass

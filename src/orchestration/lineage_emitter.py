"""Emisión de linaje hacia OpenMetadata (US-09, colaboración Héctor).

Este módulo automatiza el registro de linaje desde Airflow para las capas
Medallion (Landing -> Bronze -> Silver -> Gold). Está diseñado para ser
resiliente: si el servicio de OpenMetadata no está disponible, registra el
evento en logs sin interrumpir la corrida.

Responsable principal: Sebastián Ortiz
Colaborador (automatización desde DAG): Héctor
"""

import json
import os
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional

import requests

from src.common.config import get_settings
from src.common.logging_utils import get_logger

logger = get_logger("LineageEmitter")


@dataclass
class DatasetEdge:
    """Relación upstream -> downstream para linaje."""

    from_dataset: str
    to_dataset: str
    relationship: str = "generated_from"


@dataclass
class DatasetNode:
    """Representa un dataset en una capa Medallion."""

    name: str
    layer: str
    fully_qualified_name: str
    description: str
    path: str
    format: str = "parquet"


@dataclass
class LineageEvent:
    """Evento de linaje a emitir hacia OpenMetadata."""

    pipeline_name: str
    run_id: str
    execution_date: str
    datasets: List[DatasetNode]
    edges: List[DatasetEdge]
    metadata: Dict = None


class OpenMetadataLineageClient:
    """Cliente ligero para emitir linaje hacia OpenMetadata."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_token: Optional[str] = None,
        verify_ssl: bool = False,
        timeout_seconds: int = 10,
    ) -> None:
        self.base_url = base_url or os.getenv("OPENMETADATA_URL", "http://localhost:8585").rstrip("/")
        self.api_token = api_token or os.getenv("OPENMETADATA_API_TOKEN", "")
        self.verify_ssl = verify_ssl
        self.timeout_seconds = timeout_seconds

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
        return headers

    def emit_lineage(self, event: LineageEvent) -> bool:
        """Emite el evento de linaje a OpenMetadata (best-effort)."""
        if not self.base_url:
            logger.warning("OPENMETADATA_URL no configurado; se omite emisión de linaje.")
            return False

        payload = {
            "pipeline": event.pipeline_name,
            "runId": event.run_id,
            "executionDate": event.execution_date,
            "nodes": [asdict(d) for d in event.datasets],
            "edges": [asdict(e) for e in event.edges],
            "metadata": event.metadata or {},
        }

        url = f"{self.base_url}/api/v1/lineage"
        try:
            response = requests.post(
                url,
                headers=self._headers(),
                data=json.dumps(payload),
                timeout=self.timeout_seconds,
                verify=self.verify_ssl,
            )
            if response.status_code >= 200 and response.status_code < 300:
                logger.info(f"Linaje emitido a OpenMetadata ({url}): {len(event.edges)} relaciones")
                return True
            logger.warning(
                f"No se pudo emitir linaje a OpenMetadata (status={response.status_code}): {response.text[:200]}"
            )
            return False
        except Exception as e:
            logger.warning(f"Fallo al emitir linaje hacia {url}: {e}")
            return False


class LineageEmitter:
    """Constructor y orquestador del evento de linaje Medallion."""

    def __init__(self, client: Optional[OpenMetadataLineageClient] = None):
        self.settings = get_settings()
        self.client = client or OpenMetadataLineageClient()

    def build_medallion_lineage(self, run_id: str, execution_date: str, pipeline_name: str = "dag_medallion_pipeline") -> LineageEvent:
        """Construye el grafo de linaje Landing -> Bronze -> Silver -> Gold."""
        storage = self.settings.storage_paths

        landing_bucket = self.settings.get_bucket("landing")
        bronze_bucket = self.settings.get_bucket("bronze")
        silver_bucket = self.settings.get_bucket("silver")
        gold_bucket = self.settings.get_bucket("gold")
        quarantine_bucket = self.settings.get_bucket("quarantine")

        landing_prefix = storage.get("landing_raw", "raw")
        bronze_prefix = storage.get("bronze_parquet", "orders")
        silver_prefix = storage.get("silver_parquet", "orders")
        gold_prefix = storage.get("gold_daily_sales", "analytics/daily_sales")
        quarantine_prefix = storage.get("quarantine_path", "rejected")

        def s3_path(bucket: str, prefix: str) -> str:
            endpoint = self.settings.minio_endpoint.rstrip("/")
            return f"s3://{bucket}/{prefix}"

        datasets: List[DatasetNode] = [
            DatasetNode(
                name="landing_orders",
                layer="LANDING",
                fully_qualified_name=f"{pipeline_name}.landing.orders",
                description="Datos crudos recibidos (originales inmutables)",
                path=s3_path(landing_bucket, landing_prefix),
                format="json",
            ),
            DatasetNode(
                name="bronze_orders",
                layer="BRONZE",
                fully_qualified_name=f"{pipeline_name}.bronze.orders",
                description="Datos crudos normalizados con metadatos de auditoría (Parquet)",
                path=s3_path(bronze_bucket, bronze_prefix),
                format="parquet",
            ),
            DatasetNode(
                name="silver_orders",
                layer="SILVER",
                fully_qualified_name=f"{pipeline_name}.silver.orders",
                description="Registros limpios, tipados y deduplicados",
                path=s3_path(silver_bucket, silver_prefix),
                format="parquet",
            ),
            DatasetNode(
                name="quarantine_orders",
                layer="QUARANTINE",
                fully_qualified_name=f"{pipeline_name}.quarantine.orders",
                description="Registros rechazados por reglas de calidad (trazabilidad)",
                path=s3_path(quarantine_bucket, quarantine_prefix),
                format="parquet",
            ),
            DatasetNode(
                name="gold_daily_sales_kpis",
                layer="GOLD",
                fully_qualified_name=f"{pipeline_name}.gold.daily_sales_kpis",
                description="KPIs analíticos diarios para dashboards y chatbot (Parquet)",
                path=s3_path(gold_bucket, gold_prefix),
                format="parquet",
            ),
        ]

        edges: List[DatasetEdge] = [
            DatasetEdge(from_dataset="landing_orders", to_dataset="bronze_orders"),
            DatasetEdge(from_dataset="bronze_orders", to_dataset="silver_orders"),
            DatasetEdge(from_dataset="bronze_orders", to_dataset="quarantine_orders"),
            DatasetEdge(from_dataset="silver_orders", to_dataset="gold_daily_sales_kpis"),
        ]

        return LineageEvent(
            pipeline_name=pipeline_name,
            run_id=run_id,
            execution_date=execution_date,
            datasets=datasets,
            edges=edges,
            metadata={
                "medallion_architecture": "true",
                "dag_id": pipeline_name,
                "platform": "medallion",
            },
        )

    def emit(self, run_id: str, execution_date: str, pipeline_name: str = "dag_medallion_pipeline") -> bool:
        """Construye y emite el evento de linaje."""
        event = self.build_medallion_lineage(run_id, execution_date, pipeline_name)
        logger.info(
            f"Preparando evento de linaje: {len(event.datasets)} datasets, {len(event.edges)} relaciones (run_id={run_id})"
        )
        return self.client.emit_lineage(event)

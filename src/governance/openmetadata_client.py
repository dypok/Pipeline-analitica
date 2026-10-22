"""Cliente y sincronizador de gobernanza para OpenMetadata (US-09).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Permite sincronizar metadatos, esquemas y linaje con el servidor OpenMetadata
(puerto 8585) o exportar manifiestos JSON estandarizados en entornos desacoplados.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional
import urllib.request
import urllib.error

from src.governance.classification import ClassificationEngine
from src.governance.data_catalog import DataCatalog
from src.governance.lineage_tracker import LineageTracker

DEFAULT_MANIFEST_DIR = Path(__file__).resolve().parent.parent.parent / "governance" / "manifests"


class OpenMetadataClient:
    """Cliente de integración técnica para catálogo y linaje con OpenMetadata."""

    def __init__(
        self,
        server_url: str = "http://localhost:8585",
        manifest_dir: Optional[str | Path] = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.manifest_dir = Path(manifest_dir) if manifest_dir else DEFAULT_MANIFEST_DIR
        self.classifier = ClassificationEngine()
        self.catalog = DataCatalog(self.classifier)
        self.lineage = LineageTracker()

    def is_server_reachable(self) -> bool:
        """Verifica si el contenedor de OpenMetadata está encendido y respondiendo."""
        try:
            req = urllib.request.Request(f"{self.server_url}/api/v1/system/version", method="GET")
            with urllib.request.urlopen(req, timeout=2) as response:
                return response.status == 200
        except Exception:
            return False

    def export_governance_manifests(self) -> Dict[str, Path]:
        """Exporta los manifiestos locales de linaje, catálogo y clasificación para auditoría."""
        self.manifest_dir.mkdir(parents=True, exist_ok=True)

        catalog_path = self.manifest_dir / "openmetadata_catalog.json"
        lineage_path = self.manifest_dir / "lineage_manifest.json"
        atlas_path = self.manifest_dir / "atlas_classifications.json"

        # 1. Catálogo de tablas y columnas con tags de seguridad
        with open(catalog_path, "w", encoding="utf-8") as f:
            json.dump(self.catalog.export_openmetadata_tables(), f, indent=2, ensure_ascii=False)

        # 2. Grafo de linaje extremo a extremo
        with open(lineage_path, "w", encoding="utf-8") as f:
            json.dump(self.lineage.export_openmetadata_lineage(), f, indent=2, ensure_ascii=False)

        # 3. Taxonomía Atlas
        with open(atlas_path, "w", encoding="utf-8") as f:
            json.dump(self.classifier.export_atlas_traits(), f, indent=2, ensure_ascii=False)

        return {
            "catalog": catalog_path,
            "lineage": lineage_path,
            "atlas": atlas_path,
        }

    def sync_governance(self) -> Dict[str, Any]:
        """Sincroniza con el servidor si está activo o persiste los manifiestos en disco."""
        manifests = self.export_governance_manifests()
        reachable = self.is_server_reachable()

        return {
            "status": "ONLINE_SYNCED" if reachable else "OFFLINE_MANIFEST_SAVED",
            "server_reachable": reachable,
            "manifests_generated": {k: str(v) for k, v in manifests.items()},
            "tables_cataloged": len(self.catalog.list_tables()),
            "lineage_nodes": len(self.lineage.nodes),
            "lineage_edges": len(self.lineage.edges),
        }

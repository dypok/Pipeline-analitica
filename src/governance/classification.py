"""Módulo de clasificación y gobernanza de datos sensibles (US-09).

Responsable: Sebastián Ortiz (Ingeniero de Datos - Calidad & Gobernanza).
Permite clasificar atributos técnicos con etiquetas de seguridad:
- PII: Datos de identificación personal (email, phone, nombres).
- SENSITIVE_FINANCIAL: Datos financieros y comerciales (revenue, unit_price, montos).
- PUBLIC: Atributos abiertos de libre consumo.
Compatible con las taxonomías de Apache Atlas y OpenMetadata.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

# Ruta por defecto a la matriz de clasificación declarativa
DEFAULT_MATRIX_PATH = Path(__file__).resolve().parent.parent.parent / "governance" / "classification_matrix.json"


class ClassificationEngine:
    """Motor de clasificación de seguridad y sensibilidad de datos para el Lakehouse."""

    def __init__(self, matrix_path: Optional[str | Path] = None):
        self.matrix_path = Path(matrix_path) if matrix_path else DEFAULT_MATRIX_PATH
        self.config = self._load_matrix()
        self.entities = self.config.get("entities", {})
        self.taxonomies = self.config.get("taxonomies", {})

    def _load_matrix(self) -> Dict[str, Any]:
        """Carga la matriz de clasificación desde el archivo JSON si existe."""
        if self.matrix_path.is_file():
            try:
                with open(self.matrix_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return self._default_matrix()

    def _default_matrix(self) -> Dict[str, Any]:
        """Definición de respaldo en caso de que el archivo JSON no esté presente."""
        return {
            "taxonomies": {
                "PII": {"level": "CONFIDENTIAL", "masking_required": True},
                "SENSITIVE_FINANCIAL": {"level": "RESTRICTED", "masking_required": False},
                "PUBLIC": {"level": "PUBLIC", "masking_required": False},
            },
            "entities": {
                "customers": {
                    "first_name": "PII",
                    "last_name": "PII",
                    "email": "PII",
                    "phone": "PII",
                    "customer_id": "PUBLIC",
                },
                "orders": {
                    "total_amount": "SENSITIVE_FINANCIAL",
                    "unit_price": "SENSITIVE_FINANCIAL",
                },
                "daily_sales": {
                    "total_revenue": "SENSITIVE_FINANCIAL",
                    "avg_ticket": "SENSITIVE_FINANCIAL",
                },
            },
        }

    def get_classification(self, entity_name: str, column_name: str) -> str:
        """Retorna la etiqueta de clasificación (PII, SENSITIVE_FINANCIAL, PUBLIC)."""
        entity_cols = self.entities.get(entity_name.lower(), {})
        return entity_cols.get(column_name.lower(), "PUBLIC")

    def is_pii(self, entity_name: str, column_name: str) -> bool:
        """Determina si un atributo específico está clasificado como PII (Prueba T09.2 / P11)."""
        return self.get_classification(entity_name, column_name) == "PII"

    def is_sensitive_financial(self, entity_name: str, column_name: str) -> bool:
        """Determina si un atributo contiene información financiera confidencial."""
        return self.get_classification(entity_name, column_name) == "SENSITIVE_FINANCIAL"

    def get_entity_pii_columns(self, entity_name: str) -> List[str]:
        """Retorna la lista de columnas clasificadas como PII para una entidad."""
        entity_cols = self.entities.get(entity_name.lower(), {})
        return [col for col, tag in entity_cols.items() if tag == "PII"]

    def get_entity_sensitive_columns(self, entity_name: str) -> List[str]:
        """Retorna las columnas de sensibilidad financiera para una entidad."""
        entity_cols = self.entities.get(entity_name.lower(), {})
        return [col for col, tag in entity_cols.items() if tag == "SENSITIVE_FINANCIAL"]

    def classify_columns(self, entity_name: str, columns: List[str]) -> Dict[str, str]:
        """Clasifica una lista de nombres de columnas de una entidad dada."""
        return {col: self.get_classification(entity_name, col) for col in columns}

    def export_atlas_traits(self) -> Dict[str, Any]:
        """Genera el esquema de clasificación compatible con Apache Atlas."""
        types_def: List[Dict[str, Any]] = []
        for tag, meta in self.taxonomies.items():
            types_def.append({
                "category": "CLASSIFICATION",
                "name": tag,
                "description": meta.get("description", ""),
                "superTypes": [],
            })

        entity_mappings: Dict[str, Dict[str, List[str]]] = {}
        for ent, cols in self.entities.items():
            entity_mappings[ent] = {}
            for col, tag in cols.items():
                if tag != "PUBLIC":
                    entity_mappings[ent].setdefault(col, []).append(tag)

        return {
            "classificationDefinitions": types_def,
            "entityAttributeClassifications": entity_mappings,
        }

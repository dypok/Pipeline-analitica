"""
ingestion/json_connector.py
---------------------------
Conector de ingesta para archivos JSON y JSON Lines (US-04).
Responsable: Jefri Calderin.

Características:
- Soporta JSON estándar (lista de objetos, objeto contenedor con clave 'data') y JSON Lines (NDJSON).
- Validación de estructura jerárquica y sintaxis.
- Preservación idéntica en Landing (hash SHA-256).
- Aplanado inicial de atributos y volcado en Parquet para Bronze con columnas de auditoría.
"""

import json
import os
from pathlib import Path
from typing import Any, Optional, Union
import pandas as pd

from ingestion.base_connector import BaseConnector, IngestionConfig, IngestionResult


class JSONConnector(BaseConnector):
    """
    Conector para ingesta de archivos JSON hacia Landing y Bronze.
    """

    def __init__(
        self,
        file_path: Optional[str] = None,
        config: Optional[IngestionConfig] = None,
        json_lines: bool = False,
        data_key: Optional[str] = None
    ):
        super().__init__(config=config)
        self.file_path = file_path
        self.json_lines = json_lines
        self.data_key = data_key
        self._raw_bytes: Optional[bytes] = None
        self._parsed_data: Optional[list[dict]] = None

    def configure(self, config: dict[str, Any]) -> None:
        """Configura los parámetros del conector JSON."""
        if "file_path" in config:
            self.file_path = config["file_path"]
        if "json_lines" in config:
            self.json_lines = config["json_lines"]
        if "data_key" in config:
            self.data_key = config["data_key"]

    def validate(self) -> bool:
        """
        Valida que el archivo exista, tenga contenido y respete la sintaxis JSON/NDJSON.
        """
        if not self.file_path:
            raise ValueError("No se ha especificado 'file_path' para el conector JSON")

        p = Path(self.file_path)
        if not p.exists():
            raise FileNotFoundError(f"Archivo JSON no encontrado: {self.file_path}")

        if p.stat().st_size == 0:
            raise ValueError(f"El archivo JSON está vacío: {self.file_path}")

        with open(p, "rb") as f:
            self._raw_bytes = f.read()

        # Intentar parsear
        text = self._raw_bytes.decode("utf-8", errors="replace")

        records: list[dict] = []
        if self.json_lines or "\n" in text.strip() and not text.strip().startswith("["):
            # Modo JSON Lines
            for line_no, line in enumerate(text.strip().splitlines(), start=1):
                clean_line = line.strip()
                if not clean_line:
                    continue
                try:
                    obj = json.loads(clean_line)
                    if isinstance(obj, dict):
                        records.append(obj)
                    else:
                        raise ValueError(f"Fila {line_no} no es un objeto JSON válido")
                except json.JSONDecodeError as e:
                    raise ValueError(f"Error de sintaxis JSON Lines en línea {line_no}: {str(e)}")
        else:
            # Modo JSON estándar
            try:
                data = json.loads(text)
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict):
                    if self.data_key and self.data_key in data:
                        records = data[self.data_key]
                    elif "data" in data and isinstance(data["data"], list):
                        records = data["data"]
                    else:
                        # Objeto único
                        records = [data]
                else:
                    raise ValueError("El JSON debe ser un arreglo de objetos o un objeto clave-valor")
            except json.JSONDecodeError as e:
                raise ValueError(f"Error de sintaxis JSON en {self.file_path}: {str(e)}")

        if not records:
            raise ValueError(f"El archivo JSON no contiene registros analizables: {self.file_path}")

        self._parsed_data = records
        return True

    def fetch(self) -> bytes:
        """Retorna los bytes crudos del archivo JSON."""
        if self._raw_bytes is None:
            with open(self.file_path, "rb") as f:
                self._raw_bytes = f.read()
        return self._raw_bytes

    def save_landing(self, raw_data: bytes, source_filename: str) -> tuple[str, str]:
        """Guarda el archivo original en Landing con SHA-256."""
        source_id = self.config.source_id if self.config else "json_source"
        relative_key = self.storage.build_landing_key(
            source_id=source_id,
            ingestion_id=self.ingestion_id,
            extension="json"
        )
        return self.storage.save_landing_bytes(relative_key, raw_data)

    def save_bronze(self, data: bytes, source_filename: str) -> tuple[str, int]:
        """
        Normaliza los registros en DataFrame, aplica columnas de auditoría y guarda en Parquet.
        """
        records = self._parsed_data
        if records is None:
            text = data.decode("utf-8", errors="replace")
            records = json.loads(text)
            if isinstance(records, dict):
                records = records.get("data", [records])

        # Aplanar estructuras jerárquicas si existen
        df = pd.json_normalize(records)

        # Convertir todas las columnas a string para consistencia técnica en Bronze
        for col in df.columns:
            if df[col].dtype == object:
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x)

        df = self.add_audit_columns(df, source_filename)
        dataset_name = self.config.dataset_name if self.config else "dataset"
        relative_key = self.storage.build_bronze_key(
            dataset_name=dataset_name,
            ingestion_id=self.ingestion_id
        )

        bronze_path = self.storage.save_bronze_parquet(relative_key, df)
        return bronze_path, len(df)

    def execute(self) -> IngestionResult:
        """Ejecuta la ingesta usando el archivo configurado."""
        source_name = Path(self.file_path).name if self.file_path else "input.json"
        return self.run(source_filename=source_name)

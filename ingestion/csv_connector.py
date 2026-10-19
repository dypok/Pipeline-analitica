"""
ingestion/csv_connector.py
--------------------------
Conector de ingesta para archivos delimitados CSV (US-04).
Responsable: Jefri Calderin.

Características:
- Lectura en bloques (chunks) para escalabilidad de memoria.
- Detección y validación de delimitadores y encabezados.
- Preservación idéntica del archivo en Landing con verificación de hash SHA-256 (Prueba P01 / T04.1).
- Detección de archivos corruptos o columnas desfasadas sin emitir Bronze roto (Prueba P03 / T04.2).
- Transformación a Parquet en Bronze con columnas técnicas de auditoría.
"""

import csv
import io
import os
from pathlib import Path
from typing import Any, Optional
import pandas as pd

from ingestion.base_connector import BaseConnector, IngestionConfig, IngestionResult


class CSVConnector(BaseConnector):
    """
    Conector robusto para la ingesta de archivos CSV hacia Landing y Bronze.
    """

    def __init__(
        self,
        file_path: Optional[str] = None,
        config: Optional[IngestionConfig] = None,
        delimiter: Optional[str] = None,
        expected_columns: Optional[list[str]] = None,
        chunk_size: int = 10000
    ):
        super().__init__(config=config)
        self.file_path = file_path
        self.delimiter = delimiter
        self.expected_columns = expected_columns
        self.chunk_size = chunk_size
        self._raw_bytes: Optional[bytes] = None

    def configure(self, config: dict[str, Any]) -> None:
        """Configura los parámetros del conector CSV."""
        if "file_path" in config:
            self.file_path = config["file_path"]
        if "delimiter" in config:
            self.delimiter = config["delimiter"]
        if "expected_columns" in config:
            self.expected_columns = config["expected_columns"]
        if "chunk_size" in config:
            self.chunk_size = config["chunk_size"]

    def _detect_delimiter(self, sample_bytes: bytes) -> str:
        """Detecta automáticamente el delimitador del archivo CSV."""
        sample_text = sample_bytes[:4096].decode("utf-8", errors="ignore")
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample_text, delimiters=[",", ";", "\t", "|"])
            return dialect.delimiter
        except Exception:
            return ","

    def validate(self) -> bool:
        """
        Valida que el archivo exista, no esté vacío y contenga encabezados válidos.
        Lanza ValueError si encuentra corrupción o desfase en columnas.
        """
        if not self.file_path:
            raise ValueError("No se ha especificado 'file_path' para el conector CSV")

        p = Path(self.file_path)
        if not p.exists():
            raise FileNotFoundError(f"Archivo CSV no encontrado: {self.file_path}")

        if p.stat().st_size == 0:
            raise ValueError(f"El archivo CSV está completamente vacío: {self.file_path}")

        # Leer bytes crudos una sola vez
        with open(p, "rb") as f:
            self._raw_bytes = f.read()

        delim = self.delimiter or self._detect_delimiter(self._raw_bytes)
        self.delimiter = delim

        # Validar estructura de columnas con el lector nativo de CSV
        sample_stream = io.StringIO(self._raw_bytes.decode("utf-8", errors="replace"))
        reader = csv.reader(sample_stream, delimiter=delim)

        try:
            headers = next(reader, None)
            if not headers:
                raise ValueError("El archivo CSV no contiene fila de encabezados")

            expected_num_cols = len(headers)
            if expected_num_cols < 1:
                raise ValueError("Encabezado CSV sin columnas detectadas")

            # Si hay columnas esperadas, verificar que estén todas presentes
            if self.expected_columns:
                missing = [c for c in self.expected_columns if c not in headers]
                if missing:
                    raise ValueError(f"Faltan columnas obligatorias en el CSV: {missing}")

            # Validar consistencia de columnas en las primeras 100 filas
            for row_idx, row in enumerate(reader, start=2):
                if not row:
                    continue
                if len(row) != expected_num_cols:
                    raise ValueError(
                        f"CSV corrupto o desfasado en fila {row_idx}: se esperaban {expected_num_cols} "
                        f"columnas pero se encontraron {len(row)} (Prueba T04.2 / P03)"
                    )
                if row_idx > 200:
                    break

        except csv.Error as e:
            raise ValueError(f"Error sintáctico de formato CSV: {str(e)}")

        return True

    def fetch(self) -> bytes:
        """Retorna los bytes crudos del archivo CSV."""
        if self._raw_bytes is None:
            with open(self.file_path, "rb") as f:
                self._raw_bytes = f.read()
        return self._raw_bytes

    def save_landing(self, raw_data: bytes, source_filename: str) -> tuple[str, str]:
        """
        Guarda el archivo inalterado en Landing respetando el árbol de particiones temporales.
        Garantiza inmutabilidad verificando el hash SHA-256.
        """
        source_id = self.config.source_id if self.config else "csv_source"
        relative_key = self.storage.build_landing_key(
            source_id=source_id,
            ingestion_id=self.ingestion_id,
            extension="csv"
        )
        return self.storage.save_landing_bytes(relative_key, raw_data)

    def save_bronze(self, data: bytes, source_filename: str) -> tuple[str, int]:
        """
        Lee el contenido crudo, aplica lectura por bloques (chunks), agrega columnas técnicas de
        auditoría (_ingestion_id, _ingested_at, _source_file) y escribe en Parquet en Bronze.
        """
        delim = self.delimiter or ","
        stream = io.BytesIO(data)

        # Leer por chunks para alta eficiencia de memoria
        chunk_list = []
        total_rows = 0

        # Si pandas detecta líneas corruptas, emitirá excepción evitando Bronze roto
        for chunk in pd.read_csv(
            stream,
            delimiter=delim,
            chunksize=self.chunk_size,
            dtype=str,
            keep_default_na=False,
            on_bad_lines="error"
        ):
            chunk = self.add_audit_columns(chunk, source_filename)
            chunk_list.append(chunk)
            total_rows += len(chunk)

        if not chunk_list:
            df = pd.DataFrame()
        else:
            df = pd.concat(chunk_list, ignore_index=True)

        dataset_name = self.config.dataset_name if self.config else "dataset"
        relative_key = self.storage.build_bronze_key(
            dataset_name=dataset_name,
            ingestion_id=self.ingestion_id
        )

        bronze_path = self.storage.save_bronze_parquet(relative_key, df)
        return bronze_path, total_rows

    def execute(self) -> IngestionResult:
        """Ejecuta la ingesta usando el archivo configurado."""
        source_name = Path(self.file_path).name if self.file_path else "input.csv"
        return self.run(source_filename=source_name)

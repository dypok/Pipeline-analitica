"""Gestión de particiones e idempotencia sobre el almacenamiento de objetos (US-11).

Antes de reescribir una partición de una capa Medallion se deben limpiar los
objetos previos de esa ruta para evitar acumulación de archivos obsoletos o
filas duplicadas en reprocesamientos controlados.
"""

from typing import List, Optional

from src.common.config import get_settings
from src.common.logging_utils import get_logger
from src.common.s3_client import S3Client

logger = get_logger("PartitionManager")


class PartitionManager:
    """Limpia particiones de destino para garantizar reprocesamientos idempotentes."""

    def __init__(self, s3_client: Optional[S3Client] = None):
        self.settings = get_settings()
        self.s3_client = s3_client or S3Client()

    def clean_partition(self, layer: str, prefix: str) -> int:
        """Elimina todos los objetos de una partición (bucket de capa + prefijo).

        Devuelve el número de objetos eliminados. Es un no-op seguro si la
        partición no existe o ya está vacía.
        """
        bucket = self.settings.get_bucket(layer)
        return self.clean_prefix(bucket, prefix)

    def clean_prefix(self, bucket: str, prefix: str) -> int:
        """Elimina los objetos de un bucket bajo un prefijo dado."""
        try:
            keys: List[str] = self.s3_client.list_objects(bucket, prefix)
        except Exception as e:  # conexión no disponible: no bloquear el reproceso
            logger.warning(f"No se pudo listar la partición '{bucket}/{prefix}': {e}")
            return 0

        deleted = 0
        for key in keys:
            try:
                self.s3_client.delete_object(bucket, key)
                deleted += 1
            except Exception as e:
                logger.warning(f"No se pudo eliminar '{bucket}/{key}': {e}")

        if deleted:
            logger.info(f"Partición limpiada: {deleted} objetos eliminados de '{bucket}/{prefix}'.")
        else:
            logger.info(f"Partición '{bucket}/{prefix}' ya estaba limpia (0 objetos).")
        return deleted

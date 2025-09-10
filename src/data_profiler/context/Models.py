from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field
from venv import logger

import yaml


@dataclass(frozen=True)
class LoadConfig:
    """Configuracion base que solo contiene la ruta del archivo."""

    file_path: Path


@dataclass(frozen=True)
class CsvLoadConfig(LoadConfig):
    """Configuracion para cargar archivos CSV."""

    separator: str = ","
    encoding: str = "utf-8"


@dataclass(frozen=True)
class ExcelLoadConfig(LoadConfig):
    """Configuracion para cargar archivos Excel."""

    sheet_name: Optional[str] = None


@dataclass(frozen=True)
class TypeDetectorConfig:
    """Configuracion para detectar tipos de datos."""

    keyword_config_path: Path

    # Parametros para la deteccion de categoria
    cardinality_threshold: float = 0.05
    unique_count_limit: int = 100

    # Carga de palabras clave una sola vez al inicializar
    keywords: dict = field(init=False)

    def __post_init__(self):
        logger.info(f"Cargando palabras clave desde {self.keyword_config_path}")
        # ? Aqui entraria una excepcion si el archivo no existe o no es valido?
        with open(self.keyword_config_path, "r", encoding="utf-8") as f:
            self.keywords = yaml.safe_load(f)

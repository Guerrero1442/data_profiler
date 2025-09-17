__version__ = "0.1.0"
#* El orden de los imports es importante para evitar errores de importación circular.

# Primero importamos las excepciones para que estén disponibles al importar el paquete
from .data_profiler_exceptions import (
    DataLoaderError,
    UnsupportedFileTypeError,
    InvalidConfigurationError
)
# Luego importamos los módulos principales (clases y funciones)
from .context import (
    FileType,
    LoadConfig,
    CsvLoadConfig,
    ExcelLoadConfig,
    TypeDetectorConfig,
    Settings
)

# Finalmente importamos las clases principales
from .data_loader import DataLoader
from .type_detector import TypeDetector
from .schema_generator import SchemaGenerator

__all__ = [
    # Core
    "DataLoader",
    "TypeDetector",
    "SchemaGenerator",
    # Contexto
    "FileType",
    "LoadConfig",
    "CsvLoadConfig",
    "ExcelLoadConfig",
    "TypeDetectorConfig",
    "Settings",
    # Excepciones
    "DataLoaderError",
    "UnsupportedFileTypeError",
    "InvalidConfigurationError"
]
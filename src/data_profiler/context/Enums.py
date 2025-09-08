# Crear el Enum para los tipos de archivo soportados
from enum import Enum

from data_profiler import UnsupportedFileTypeError


class FileType(Enum):
    """Define los tipos de archivo soportados."""
    CSV = '.csv'
    TXT = '.txt'
    EXCEL = '.xlsx'
    JSON = '.json'
    PARQUET = '.parquet'
    
    @classmethod
    def from_extension(cls, extension: str):
        """Devuelve el FileType correspondiente a la extensión dada."""
        ext_lower = extension.lower()
        for member in cls:
            if member.value == ext_lower:
                return member
        raise UnsupportedFileTypeError(f"Tipo de archivo no soportado: {extension}")
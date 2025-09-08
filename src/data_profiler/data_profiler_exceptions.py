class DataLoaderError(Exception):
    """Excepción personalizada para errores en DataLoader."""
    pass

class UnsupportedFileTypeError(DataLoaderError):
    """Se lanza cuando la extensión del archivo no es soportada."""
    pass

class InvalidConfigurationError(DataLoaderError):
    """Se lanza cuando la configuración proporcionada es inválida."""
    pass
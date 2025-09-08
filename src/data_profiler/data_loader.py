import pandas as pd
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Optional, Callable, Type
from loguru import logger
from data_profiler import DataLoaderError, UnsupportedFileTypeError, FileType, InvalidConfigurationError
from pandas.errors import ParserError


@dataclass(frozen=True)
class LoadConfig:
    """Configuracion base que solo contiene la ruta del archivo."""
    file_path: Path
    
@dataclass(frozen=True)
class CsvLoadConfig(LoadConfig):
    """Configuracion para cargar archivos CSV."""
    separator: str = ','
    encoding: str = 'utf-8'
    
@dataclass(frozen=True)
class ExcelLoadConfig(LoadConfig):
    """Configuracion para cargar archivos Excel."""
    sheet_name: Optional[str] = None
    

class DataLoader:
    def __init__(self, config: LoadConfig):
        if not config.file_path.exists():
            raise FileNotFoundError(f"El archivo {config.file_path} no existe.")
        self.config = config
        
        #? Repasar este diccionario
        self._loaders: Dict[Type[LoadConfig], Callable[[], pd.DataFrame]] = {
            CsvLoadConfig: self._load_csv,
            ExcelLoadConfig: self._load_excel,
            LoadConfig: self._load_simple,
        }
        
    def load(self) -> pd.DataFrame:
        logger.info(f"iniciando la carga de datos desde {self.config.file_path}")
        
        for config_type, loader in self._loaders.items():
            if isinstance(self.config, config_type):
                return loader()
            
        raise UnsupportedFileTypeError(f"No hay un cargador definido para la configuración: {type(self.config).__name__}")

    def _load_csv(self) -> pd.DataFrame:
        
        assert isinstance(self.config, CsvLoadConfig)
        
        logger.info(f"Leyendo CSV con separador '{self.config.separator}' desde {self.config.file_path}")
        try:
            return pd.read_csv(self.config.file_path, 
                               delimiter=self.config.separator, 
                               encoding=self.config.encoding
                               )
        except UnicodeDecodeError as e:
            raise InvalidConfigurationError(
                f"No se pudo decodificar el archivo con '{self.config.encoding}'. Intenta con 'latin-1' o 'cp1252'."
            ) from e
        except ParserError as e:
            raise InvalidConfigurationError(f"Error al parsear el archivo CSV con el separador recibido: {e}") from e

    def _load_excel(self) -> pd.DataFrame:
        assert isinstance(self.config, ExcelLoadConfig)
        
        if self.config.sheet_name:
            logger.info(f"Leyendo la hoja '{self.config.sheet_name}' desde {self.config.file_path}")
            try:
                return pd.read_excel(self.config.file_path, sheet_name=self.config.sheet_name)
            except ValueError as e:
                raise InvalidConfigurationError(f"La hoja '{self.config.sheet_name}' no existe en el archivo Excel.") from e
        
        logger.info(f"Leyendo todas las hojas desde {self.config.file_path}")
        xls = pd.ExcelFile(self.config.file_path)
        all_sheets_df = [pd.read_excel(xls, sheet_name=sheet) for sheet in xls.sheet_names]
        
        return pd.concat(all_sheets_df, ignore_index=True)

    def _load_simple(self) -> pd.DataFrame:
        """Carga archivos que no requieren parametros extra, como JSON o Parquet."""
        file_type = FileType.from_extension(self.config.file_path.suffix)
        
        simple_loaders = {
            FileType.JSON: pd.read_json,
            FileType.PARQUET: pd.read_parquet,
        }
        
        reader = simple_loaders.get(file_type)
        
        if reader:
            logger.info(f"Leyendo {file_type.name} desde {self.config.file_path}")
            return reader(self.config.file_path)
        
        raise UnsupportedFileTypeError(f"No hay un cargador definido para el tipo de archivo: {file_type.name}")  
    
def main_interactive():
    """
    Función que maneja la interacción con el usuario y luego utiliza
    el DataLoader para hacer el trabajo pesado.
    """
    from tkinter import filedialog
    import tkinter as tk
    import sys

    # --- 3. Configuración de Loguru (Opcional pero recomendado) ---
    # Esto te da control total sobre cómo se ven y a dónde van los logs.
    logger.remove() # Remueve la configuración por defecto
    logger.add(
        sys.stderr, # Envía los logs a la consola
        level="INFO", # Muestra mensajes desde INFO hacia arriba (INFO, SUCCESS, WARNING, ERROR)
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
        colorize=True,
    )
    logger.add("file_{time}.log", level="DEBUG") # Guarda todo (incluyendo DEBUG) en un archivo

    root = tk.Tk()
    root.withdraw()
    
    file_path_str = filedialog.askopenfilename(title="Seleccionar archivo de datos")
    if not file_path_str:
        logger.warning("No se seleccionó ningún archivo.")
        return

    file_path = Path(file_path_str)

    try:
        # --- Primer Intento de Carga ---
        file_type = FileType.from_extension(file_path.suffix)
        config: LoadConfig
        
        if file_type in [FileType.CSV, FileType.TXT]:
            separator = input("Ingresa el separador para el archivo CSV (por defecto es ','): ") or ','
            encoding = input("Ingresa la codificación del archivo (por defecto es 'utf-8'): ") or 'utf-8'
            config = CsvLoadConfig(file_path=file_path, separator=separator, encoding=encoding)
        elif file_type == FileType.EXCEL:
            xls = pd.ExcelFile(file_path)
            if len(xls.sheet_names) > 1:
                logger.info(f"Hojas disponibles en el archivo Excel: {xls.sheet_names}")
                sheet_name = input("Ingresa el nombre de la hoja a cargar (deja vacío para todas): ") or None
                config = ExcelLoadConfig(file_path=file_path, sheet_name=sheet_name)
            else:
                config = ExcelLoadConfig(file_path=file_path)
        else:
            config = LoadConfig(file_path=file_path)
            
        loader = DataLoader(config)
        datos = loader.load()

        logger.success("Datos cargados exitosamente:")
        print(datos.head())

    except InvalidConfigurationError as e:
        logger.error(f"Error de configuración: {e}")
    except UnsupportedFileTypeError as e:
        logger.error(f"Error: {e}")
    except FileNotFoundError as e:
        logger.error(f'Error: {e}')
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado: {e}")
        

if __name__ == "__main__":
    main_interactive()
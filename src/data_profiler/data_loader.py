import pandas as pd
from typing import Dict, Callable, Type
from loguru import logger
from pandas.errors import ParserError

from data_profiler import (
    FileType,
    LoadConfig,
    CsvLoadConfig,
    ExcelLoadConfig,
    UnsupportedFileTypeError,
    InvalidConfigurationError
)

class DataLoader:
    def __init__(self, config: LoadConfig):
        if not config.file_path.exists():
            raise FileNotFoundError(f"El archivo {config.file_path} no existe.")
        self.config = config

        # ? Repasar este diccionario
        self._loaders: Dict[Type[LoadConfig], Callable[[], pd.DataFrame]] = {
            CsvLoadConfig: self._load_csv,
            ExcelLoadConfig: self._load_excel,
            LoadConfig: self._load_simple,
        }

    def load(self) -> pd.DataFrame:
        #! Cambiar comentarios a ingles
        logger.info(f"iniciando la carga de datos desde {self.config.file_path}")

        for config_type, loader in self._loaders.items():
            if isinstance(self.config, config_type):
                return loader()

        raise UnsupportedFileTypeError(
            f"No hay un cargador definido para la configuración: {type(self.config).__name__}"
        )

    def _load_csv(self) -> pd.DataFrame:
        assert isinstance(self.config, CsvLoadConfig) #! evitar assert 

        logger.info(
            f"Leyendo CSV con separador '{self.config.separator}' desde {self.config.file_path}"
        )
        try:
            csv_df = pd.read_csv(
                self.config.file_path,
                delimiter=self.config.separator,
                encoding=self.config.encoding,
                engine="pyarrow",
                dtype_backend="pyarrow"
            )
            
            if csv_df.shape[1] <= 1:
                raise InvalidConfigurationError(
                    "El archivo CSV parece tener un solo campo. Verifica el separador."
                )
            return csv_df     
        except UnicodeDecodeError as e:
            raise InvalidConfigurationError(
                f"No se pudo decodificar el archivo con '{self.config.encoding}'. Intenta con 'latin-1' o 'cp1252'."
            ) from e
        except ParserError as e:
            raise InvalidConfigurationError(
                f"Error al parsear el archivo CSV con el separador recibido: {e}"
            ) from e

    def _load_excel(self) -> pd.DataFrame:
        assert isinstance(self.config, ExcelLoadConfig)
        
        if self.config.sheet_name:
            logger.info(
                f"Leyendo la hoja '{self.config.sheet_name}' desde {self.config.file_path}"
            )
            try:
                return pd.read_excel(
                    self.config.file_path, sheet_name=self.config.sheet_name
                )
            except ValueError as e:
                raise InvalidConfigurationError(
                    f"La hoja '{self.config.sheet_name}' no existe en el archivo Excel."
                ) from e

        logger.info(f"Leyendo todas las hojas desde {self.config.file_path}")
        xls = pd.ExcelFile(self.config.file_path)
        all_sheets_df = [
            pd.read_excel(xls, sheet_name=sheet) for sheet in xls.sheet_names
        ]

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

        raise UnsupportedFileTypeError(
            f"No hay un cargador definido para el tipo de archivo: {file_type.name}"
        )
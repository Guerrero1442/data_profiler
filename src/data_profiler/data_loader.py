import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Optional, Callable, Type
from loguru import logger
from .data_profiler_exceptions import (
    UnsupportedFileTypeError,
    InvalidConfigurationError,
)
from .context import FileType, LoadConfig, CsvLoadConfig, ExcelLoadConfig

from pandas.errors import ParserError


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
        logger.info(f"iniciando la carga de datos desde {self.config.file_path}")

        for config_type, loader in self._loaders.items():
            if isinstance(self.config, config_type):
                return loader()

        raise UnsupportedFileTypeError(
            f"No hay un cargador definido para la configuración: {type(self.config).__name__}"
        )

    def _load_csv(self) -> pd.DataFrame:
        assert isinstance(self.config, CsvLoadConfig)

        logger.info(
            f"Leyendo CSV con separador '{self.config.separator}' desde {self.config.file_path}"
        )
        try:
            return pd.read_csv(
                self.config.file_path,
                delimiter=self.config.separator,
                encoding=self.config.encoding,
            )
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


def load_data_interactive() -> Optional[pd.DataFrame]:
    from tkinter import filedialog, Tk

    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)

    root = Tk()
    root.withdraw()

    file_path_str = filedialog.askopenfilename(title="Selecciona un archivo de datos")
    if not file_path_str:
        logger.warning("No se seleccionó ningún archivo.")
        return None

    file_path = Path(file_path_str)
    config: LoadConfig

    try:
        file_type = FileType.from_extension(file_path.suffix)
        if file_type is None:
            raise UnsupportedFileTypeError(
                f"Tipo de archivo no soportado: {file_path.suffix}"
            )

        if file_type in [FileType.CSV, FileType.TXT]:
            separator = input("Ingresa el separador (por defecto es ','): ") or ","
            encoding = (
                input("Ingresa la codificación (por defecto es 'utf-8'): ") or "utf-8"
            )
            config = CsvLoadConfig(
                file_path=file_path, separator=separator, encoding=encoding
            )
        elif file_type in [FileType.XLSX, FileType.XLS]:
            xls = pd.ExcelFile(file_path)
            if len(xls.sheet_names) > 1:
                print("Hojas disponibles:")
                for idx, sheet in enumerate(xls.sheet_names):
                    print(f"{idx + 1}. {sheet}")
                sheet_choice = input(
                    "Selecciona una hoja por número (o presiona Enter para cargar todas) si no es valido consolida todo: "
                )
                sheet_name = (
                    xls.sheet_names[int(sheet_choice) - 1]
                    if sheet_choice.isdigit()
                    and 1 <= int(sheet_choice) <= len(xls.sheet_names)
                    else None
                )
            config = ExcelLoadConfig(file_path=file_path, sheet_name=sheet_name)
        else:
            config = LoadConfig(file_path=file_path)

        loader = DataLoader(config)

        datos = loader.load()
        logger.success(
            f"Datos cargados exitosamente desde {file_path} con {len(datos)} filas y {len(datos.columns)} columnas."
        )
        return datos

    except (
        UnsupportedFileTypeError,
        InvalidConfigurationError,
        FileNotFoundError,
    ) as e:
        logger.error(e)
        return None
    except Exception as e:
        logger.exception(f"Ocurrió un error inesperado: {e}")
        return None

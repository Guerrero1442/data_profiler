from typing import Optional
from pydantic import BaseModel, Field, FilePath, DirectoryPath
from pydantic_settings import BaseSettings, SettingsConfigDict
import yaml
from loguru import logger
from dataclasses import dataclass

# 1. Mantenemos los modelos de configuración, pero ahora con Pydantic
#    Son muy similares a los dataclasses, pero con más poder.

class LoadConfig(BaseModel):
    """Configuración base que solo contiene la ruta del archivo."""
    file_path: FilePath # Pydantic valida que el archivo exista

class CsvLoadConfig(LoadConfig):
    """Configuración para cargar archivos CSV."""
    separator: str = ","
    encoding: str = "utf-8"

class ExcelLoadConfig(LoadConfig):
    """Configuración para cargar archivos Excel."""
    sheet_name: Optional[str] = None


# 2. Reemplazamos el dataclass TypeDetectorConfig con un BaseModel de Pydantic

@dataclass(frozen=True)
class TypeDetectorConfig(BaseModel):
    """Configuracion para detectar tipos de datos."""
    
    keyword_config_path: FilePath
    cardinality_threshold: float = 0.05
    unique_count_limit: int = 100
    keywords: dict = {}

    # Pydantic puede cargar y validar datos de forma más elegante
    @classmethod
    def from_yaml(cls, config_path: str):
        """Crea una instancia de TypeDetectorConfig desde un archivo YAML."""
        logger.info(f"Cargando palabras clave desde {config_path}")
        try:
            with open(config_path, "r", encoding="utf-8") as f:
                keywords_data = yaml.safe_load(f)
            return cls(keyword_config_path=config_path, keywords=keywords_data, cardinality_threshold=0.05, unique_count_limit=100)
        except FileNotFoundError:
            logger.error(f"Archivo de configuración no encontrado: {config_path}")
            return cls(keyword_config_path=config_path, keywords={})
        except yaml.YAMLError as e:
            logger.error(f"Error al parsear YAML: {e}")
            return cls(keyword_config_path=config_path, keywords={})


# 3. (La gran mejora) Creamos una clase para gestionar TODAS las variables de entorno.

class Settings(BaseSettings):
    """
    Gestiona la configuración de la aplicación a través de variables de entorno.
    Pydantic leerá automáticamente las variables con estos nombres.
    """
    # Para cargar el archivo de datos
    data_directory_path: DirectoryPath = Field(..., description="Ruta al directorio que contiene los archivos de datos")

    output_directory_path: DirectoryPath = Field("output_schemas", description="Ruta al directorio de salida para esquemas y DDL")

    data_separator: str = Field(",", description="Separador para archivos CSV/TXT")
    data_encoding: str = Field("utf-8", description="Codificación para archivos CSV/TXT")
    data_sheet_name: Optional[str] = Field(None, description="Nombre de la hoja para archivos Excel")

    # Para el TypeDetector
    keyword_config_path: FilePath = Field(..., description="Ruta al archivo YAML de palabras clave")
    
    # Para BigQuery
    bigquery_project_id: Optional[str] = Field(None, description="ID del proyecto de BigQuery")
    bigquery_dataset_id: Optional[str] = Field(None, description="ID del dataset de BigQuery")
    
    log_level: str = Field("INFO", description="Nivel de log: DEBUG, INFO, WARNING, ERROR")
    
    # Configuración para que Pydantic lea desde un archivo .env (opcional, pero útil para desarrollo)
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')
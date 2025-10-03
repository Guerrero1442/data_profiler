from pathlib import Path
from loguru import logger
from data_profiler import (
    Settings,
    TypeDetectorConfig,
    TypeDetector,
    FileType,
    CsvLoadConfig,
    ExcelLoadConfig,
    LoadConfig,
    DataLoader,
    SchemaGenerator,
    OracleDialect,
    BigQueryDialect,
)
from pydantic import ValidationError

def setup_logger(file_path: Path, settings: Settings):
    """
    Configura un logger de loguru para registrar mensajes en un archivo y en la consola.
    """
    log_file_path = settings.output_directory_path / f"{file_path.stem}.log"
    logger.remove()  # Elimina cualquier configuración previa del logger
    logger.add(
        log_file_path,
        level=settings.log_level,
        format="{time} {level} {message}",
        encoding="utf-8",
        mode="w",
    )


def process_file(file_path: Path, settings: Settings):
    setup_logger(file_path, settings)
    
    logger.info(f"Procesando archivo: {file_path.name}")

    try:
        file_type = FileType.from_extension(file_path.suffix)

        if file_type in [FileType.TXT, FileType.CSV]:
            config = CsvLoadConfig(
                file_path=file_path,
                separator=settings.data_separator,
                encoding=settings.data_encoding,
            )
        elif file_type in [FileType.XLSX, FileType.XLS]:
            config = ExcelLoadConfig(
                file_path=file_path, sheet_name=settings.data_sheet_name
            )
        else:
            config = LoadConfig(file_path=file_path)

        loader = DataLoader(config)
        datos = loader.load()
        logger.success(
            f"Datos cargados exitosamente desde {file_path.name} con {len(datos)} filas y {len(datos.columns)} columnas."
        )
        
        # 2. Detectar tipos de datos
        detector_config = TypeDetectorConfig.from_yaml(settings.keyword_config_path)
        detector = TypeDetector(datos, detector_config)
        df_optimizado = detector.run_detection()
        df_optimizado.columns = df_optimizado.columns.str.replace('\n', ' ').str.strip()
        
        # 3. Generacion de esquema y DDL
        dialecto_bigquery = BigQueryDialect(
            project_id=settings.bigquery_project_id,
            dataset_id=settings.bigquery_dataset_id
        )
        schema_gen = SchemaGenerator(df_optimizado, dialect=dialecto_bigquery)
        
        # Define nombres de salida dinamicos basados en el nombre del archivo de entrada
        output_base_name = file_path.stem
        output_dir = settings.output_directory_path
        output_dir.mkdir(exist_ok=True)
        
        schema_excel_path = output_dir / f"{output_base_name}_schema.xlsx"
        ddl_sql_path = output_dir / f"{output_base_name}_schema.sql"
        
        schema_gen.to_excel(schema_excel_path)
        logger.success(f"Esquema guardado en {schema_excel_path}")

        schema_gen.to_ddl_file(output_base_name, ddl_sql_path)
        logger.success(f"DDL guardado en {ddl_sql_path}")
        
    except ValidationError as e:
        logger.error(f"Error de validación en la configuración para {file_path.name}: {e}")
        return
    except Exception as e:
        logger.error(f"Error inesperado en {file_path.name}: {e}")
        return


def main():
    
    # Flujo 1: Usar la función interactiva de alto nivel
    try: 
        settings = Settings()
    except ValidationError as e:
        logger.error(f"Error de validación en las variables de entorno: {e}")
        return

    directory = settings.data_directory_path
    logger.info(f"Buscando archivos en el directorio: {directory}")
    
    for filename in Path(directory).iterdir():
        if filename.name.startswith('.') or filename.name.lower() == 'desktop.ini':
            continue
        file_path = directory / filename
        if file_path.is_file():
            process_file(file_path, settings)
        else:
            logger.warning(f"{file_path} no es un archivo. Se omitirá.")

if __name__ == "__main__":
    main()

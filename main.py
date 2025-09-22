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
    OracleDialect
)
from pydantic import ValidationError

def main():
    try:
        settings = Settings()   
        
        file_path = settings.data_file_path
        
        file_type = FileType.from_extension(file_path.suffix)
        
        if file_type in [FileType.TXT, FileType.CSV]:
            config = CsvLoadConfig(file_path=file_path, separator=settings.data_separator, encoding=settings.data_encoding)
        elif file_type in [FileType.XLSX, FileType.XLS]:
            config = ExcelLoadConfig(file_path=file_path, sheet_name=settings.data_sheet_name)
        else:
            config = LoadConfig(file_path=file_path)
            
        loader = DataLoader(config)
        datos = loader.load()
        
        logger.success(f"Datos cargados exitosamente desde {file_path} con {len(datos)} filas y {len(datos.columns)} columnas.")
    except ValidationError as e:
        logger.error(f"Error de validación en la configuración: {e}")
        return
    except Exception as e:
        logger.error(f"Error inesperado: {e}")
        return
        
    # Flujo 1: Usar la función interactiva de alto nivel
    logger.info("Iniciando flujo interactivo...")
    
    if datos is None or datos.empty:
        logger.error("No se cargaron datos. Terminando el programa.")
        return
    logger.info("Datos cargados exitosamente en el flujo interactivo.")

    config_path = "config/column_keywords.yaml"
    detector_config = TypeDetectorConfig.from_yaml(config_path)

    
    detector = TypeDetector(datos, detector_config)
    df_optimizado = detector.run_detection()

    logger.info("Tipos y categorias detectados en el flujo interactivo.")
    print(df_optimizado.dtypes) 
    
    df_optimizado.to_csv("data_optimizado.csv", index=False)

    # eliminar saltos de lineas de los nombres de columnas
    df_optimizado.columns = df_optimizado.columns.str.replace('\n', ' ').str.strip()

    dialecto_oracle = OracleDialect()

    # Schema generation
    schema_gen = SchemaGenerator(df_optimizado, dialect=dialecto_oracle)

    # Generar y guardar archivo Excel
    schema_gen.to_excel("schema.xlsx")
    logger.info("Esquema exportado a schema.xlsx")

    # Generar y guardar DDL para Oracle en un archivo .sql
    table_name = "nt_unicos"
    output_sql_path = "schema.sql"

    # Generar y mostrar DDL para Oracle
    schema_gen.to_ddl_file(table_name, output_sql_path)
    
if __name__ == "__main__":
    main()

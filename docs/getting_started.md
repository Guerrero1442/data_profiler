
## Instalación

Instala `data-profiler` directamente desde PyPI (una vez que lo publiques). La biblioteca requiere Python 3.11 o superior.

```bash
pip install data-profiler
````

## Configuración

Para utilizar **Data Profiler**, necesitarás un archivo de configuración para las palabras clave de las columnas.

1.  **Crea un archivo `column_keywords.yaml`:**

    En tu proyecto, crea un directorio `config` y dentro de él un archivo llamado `column_keywords.yaml`. Este archivo te permitirá definir palabras clave para forzar la detección de tipos de datos de texto y numéricos.

    ```yaml
    text_keywords:
      - "documento"
      - "identificacion"
      - "codigo"
    float_keywords:
      - "costo"
      - "valor"
    date_formats:
      - "%Y-%m-%d"
      - "%d/%m/%Y"
    ```

2.  **Configura las variables de entorno (opcional):**

    Puedes usar un archivo `.env` para gestionar la configuración de la ruta de tus datos y las credenciales de la base de datos.

    ```
    DATA_FILE_PATH="ruta/a/tu/archivo.csv"
    KEYWORD_CONFIG_PATH="config/column_keywords.yaml"
    BIGQUERY_PROJECT_ID="tu-proyecto-gcp"
    BIGQUERY_DATASET_ID="tu_dataset"
    ```

-----

## Ejemplo de Uso

### Perfilado Básico de un Archivo CSV

Este ejemplo muestra cómo inicializar el `DataLoader`, ejecutar el `TypeDetector` y generar un esquema.

```python
from data_profiler import (
    Settings,
    TypeDetectorConfig,
    TypeDetector,
    DataLoader,
    CsvLoadConfig,
    SchemaGenerator,
    BigQueryDialect
)

# --- 1. Configuración ---
settings = Settings()
config = CsvLoadConfig(
    file_path=settings.data_file_path,
    separator=settings.data_separator,
    encoding=settings.data_encoding
)

# --- 2. Carga de Datos ---
loader = DataLoader(config)
datos = loader.load()

# --- 3. Detección de Tipos ---
detector_config = TypeDetectorConfig.from_yaml(settings.keyword_config_path)
detector = TypeDetector(datos, detector_config)
df_optimizado = detector.run_detection()

# --- 4. Generación de Esquema ---
dialecto_bigquery = BigQueryDialect(
    project_id=settings.bigquery_project_id,
    dataset_id=settings.bigquery_dataset_id
)
schema_gen = SchemaGenerator(df_optimizado, dialect=dialecto_bigquery)

# Generar y guardar el esquema en Excel y DDL
schema_gen.to_excel("schema.xlsx")
schema_gen.to_ddl_file("nombre_tabla", "schema.sql")

print("¡Perfilado de datos completado!")

```

-----

### Contribuciones

¡Las contribuciones son bienvenidas\! Si tienes una solicitud de función, un informe de error o una pull request, por favor abre un issue o una PR en el [repositorio de GitHub](https://www.google.com/url?sa=E&source=gmail&q=https://github.com/guerrero1442/data_profiler).

### Licencia

Este proyecto está licenciado bajo la Licencia MIT. Consulta el archivo `LICENSE` para más detalles.

````


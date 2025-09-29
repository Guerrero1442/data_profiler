# tests/test_schema_generator.py
import pytest
import pandas as pd
import pyarrow as pa
import numpy as np
from pathlib import Path
from data_profiler import SchemaGenerator, OracleDialect, BigQueryDialect

# 1. Fixture para crear un DataFrame optimizado de prueba
#    Este DataFrame simula la salida del TypeDetector.
@pytest.fixture
def optimized_dataframe() -> pd.DataFrame:
    """Crea un DataFrame de prueba con tipos de datos ya optimizados."""
    data = {
        "id_usuario": pd.Series([1, 2, 3, 4, 5], dtype="Int64"),
        "monto_compra": pd.Series([199.99, 1500.50, 45.00, 2500.75, 999.00], dtype="float64"),
        "fecha_registro": pd.to_datetime(["2024-01-10", "2024-02-15", "2024-03-20", "2024-04-25", "2024-05-30"]),
        "fecha_ultima_compra": pd.to_datetime([
            "2024-06-01 14:30:00", "2024-06-15 09:15:00", "2024-07-20 18:45:00",
            "2024-08-05 12:00:00", "2024-09-10 16:20:00"
        ]),
        "pais": pd.Series(["CO", "MX", "CO", "PE", "MX"], dtype="category"),
        "descripcion_larga": pd.Series([
            "producto A", "producto B con extra", "producto C", np.nan, "producto D"
        ], dtype="string"),
        "codigo_fijo": pd.Series(["AB123", "CD456", "EF789", "GH012", "IJ345"], dtype="string"),
        "valores_nulos": pd.Series([np.nan, np.nan, np.nan, np.nan, np.nan], dtype="string"),
        "cadenas_vacias": pd.Series(["", "", "", "", ""], dtype="string"),
    }
    
    # convertir a pyarrow
    data["fecha_registro"] = data["fecha_registro"].astype(pd.ArrowDtype(pa.date32()))
    data["fecha_ultima_compra"] = data["fecha_ultima_compra"].astype(pd.ArrowDtype(pa.timestamp("s")))
    
    return pd.DataFrame(data)

@pytest.fixture
def dialecto_oracle() -> OracleDialect:
    """Proporciona una instancia del dialecto Oracle."""
    return OracleDialect()

@pytest.fixture
def dialecto_bigquery() -> BigQueryDialect:
    """Proporciona una instancia del dialecto BigQuery."""
    return BigQueryDialect()

# 2. Pruebas para SchemaGenerator
def test_generate_schema_dict_correctly_identifies_types_oracle(optimized_dataframe: pd.DataFrame, dialecto_oracle: OracleDialect):
    """
    Verifica que el diccionario generado contenga los tipos de Oracle,
    longitudes y obligatoriedad correctos para cada columna.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe, dialect=dialecto_oracle)

    # Act
    schema = schema_gen.generate_schema_dict()

    # Assert
    assert isinstance(schema, dict)
    assert "id_usuario" in schema
    
    # Verificamos los tipos de datos inferidos para Oracle
    assert schema["id_usuario"]["tipo"] == "NUMBER(1)"
    assert schema["monto_compra"]["tipo"] == "NUMBER(6, 2)" # 4 enteros + 2 decimales
    assert schema["fecha_registro"]["tipo"] == "DATE"
    assert schema["fecha_ultima_compra"]["tipo"] == "TIMESTAMP"
    assert schema["pais"]["tipo"] == "CHAR(2)"
    assert schema["descripcion_larga"]["tipo"] == "VARCHAR2(20)"
    assert schema["codigo_fijo"]["tipo"] == "CHAR(5)" # Como todos tienen la misma longitud, debería ser CHAR
    assert schema["valores_nulos"]["tipo"] == "VARCHAR2(1)" # Columna con solo nulos
    assert schema["cadenas_vacias"]["tipo"] == "VARCHAR2(1)" # Columna con solo cadenas vacías
    
    # Verificamos la obligatoriedad
    assert schema["id_usuario"]["obligatoria"] == "Obligatorio"
    assert schema["descripcion_larga"]["obligatoria"] == "No Obligatorio"

def test_to_ddl_file_creates_correct_oracle_statement(optimized_dataframe: pd.DataFrame, tmp_path: Path, dialecto_oracle: OracleDialect):
    """
    Verifica que el método to_ddl_file cree un archivo DDL con la sintaxis
    y los tipos de datos correctos para Oracle.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe, dialect=dialecto_oracle)
    table_name = "CLIENTES_PROCESADOS"
    output_path = tmp_path / "schema_ddl.sql"

    # Act
    schema_gen.to_ddl_file(table_name, output_path)

    # Assert
    # Verificar que el archivo se creó
    assert output_path.exists()
    assert output_path.is_file()
    
    # Leer el contenido del archivo
    with open(output_path, 'r', encoding='utf-8') as f:
        ddl_content = f.read()
    
    # Verificar el contenido del DDL
    assert f"CREATE TABLE {table_name} (" in ddl_content
    assert '"id_usuario" NUMBER(1)' in ddl_content
    assert '"monto_compra" NUMBER(6, 2)' in ddl_content
    assert '"fecha_registro" DATE' in ddl_content
    assert '"pais" CHAR(2)' in ddl_content
    assert '"descripcion_larga" VARCHAR2(20)' in ddl_content
    assert '"codigo_fijo" CHAR(5)' in ddl_content
    assert ddl_content.strip().endswith(");")
    
    # Verificar que contiene comentario
    assert f"-- Esquema generado para la tabla {table_name}" in ddl_content
    
def test_generate_schema_dict_correctly_identifies_types_bigquery(optimized_dataframe: pd.DataFrame, dialecto_bigquery: BigQueryDialect):
    """
    Verifica que el diccionario generado contenga los tipos de BigQuery correctos.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe, dialect=dialecto_bigquery)

    # Act
    schema = schema_gen.generate_schema_dict()

    # Assert
    assert schema["id_usuario"]["tipo"] == "INTEGER"
    assert schema["monto_compra"]["tipo"] == "NUMERIC"
    assert schema["fecha_registro"]["tipo"] == "DATE"
    assert schema["fecha_ultima_compra"]["tipo"] == "TIMESTAMP"
    assert schema["pais"]["tipo"] == "STRING"
    assert schema["descripcion_larga"]["tipo"] == "STRING"
    
def test_to_ddl_file_creates_correct_bigquery_statement(optimized_dataframe: pd.DataFrame, tmp_path: Path, dialecto_bigquery: BigQueryDialect):
    """
    Verifica que el método to_ddl_file cree un archivo DDL con la sintaxis
    y los tipos de datos correctos para BigQuery.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe, dialect=dialecto_bigquery)
    table_name = "CLIENTES_PROCESADOS"
    output_path = tmp_path / "schema_ddl_bq.sql"

    # Act
    schema_gen.to_ddl_file(table_name, output_path)

    # Assert
    # Verificar que el archivo se creó
    assert output_path.exists()
    assert output_path.is_file()
    
    # Leer el contenido del archivo
    with open(output_path, 'r', encoding='utf-8') as f:
        ddl_content = f.read()
    
    # Verificar el contenido del DDL
    assert f"CREATE OR REPLACE TABLE `{table_name}` (" in ddl_content
    assert '`id_usuario` INTEGER' in ddl_content
    assert '`monto_compra` NUMERIC' in ddl_content
    assert '`fecha_registro` DATE' in ddl_content
    assert '`pais` STRING' in ddl_content
    assert '`descripcion_larga` STRING' in ddl_content
    assert ddl_content.strip().endswith(");")
    
    # Verificar que contiene comentario
    assert f"-- Esquema generado para la tabla {table_name} (BigQuery)" in ddl_content

def test_to_excel_creates_file(optimized_dataframe: pd.DataFrame, tmp_path: Path, dialecto_oracle: OracleDialect):
    """
    Verifica que el método to_excel cree un archivo en la ruta especificada.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe, dialect=dialecto_oracle)
    output_path = tmp_path / "schema_test.xlsx"

    # Act
    schema_gen.to_excel(output_path)

    # Assert
    assert output_path.exists()
    assert output_path.is_file()
    
    # Prueba adicional (opcional pero recomendada): leer el archivo y verificar su contenido
    df_from_excel = pd.read_excel(output_path, index_col=0)
    assert "tipo" in df_from_excel.columns
    assert df_from_excel.loc["id_usuario"]["tipo"] == "NUMBER(1)"
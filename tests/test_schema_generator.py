# tests/test_schema_generator.py
import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from data_profiler import SchemaGenerator

# 1. Fixture para crear un DataFrame optimizado de prueba
#    Este DataFrame simula la salida del TypeDetector.
@pytest.fixture
def optimized_dataframe() -> pd.DataFrame:
    """Crea un DataFrame de prueba con tipos de datos ya optimizados."""
    data = {
        "id_usuario": pd.Series([1, 2, 3, 4, 5], dtype="Int64"),
        "monto_compra": pd.Series([199.99, 1500.50, 45.00, 2500.75, 999.00], dtype="float64"),
        "fecha_registro": pd.to_datetime(["2024-01-10", "2024-02-15", "2024-03-20", "2024-04-25", "2024-05-30"]),
        "pais": pd.Series(["CO", "MX", "CO", "PE", "MX"], dtype="category"),
        "descripcion_larga": pd.Series([
            "producto A", "producto B con extra", "producto C", np.nan, "producto D"
        ], dtype="string"),
        "codigo_fijo": pd.Series(["AB123", "CD456", "EF789", "GH012", "IJ345"], dtype="string")
    }
    return pd.DataFrame(data)

# 2. Pruebas para SchemaGenerator

def test_generate_schema_dict_correctly_identifies_types(optimized_dataframe: pd.DataFrame):
    """
    Verifica que el diccionario generado contenga los tipos de Oracle,
    longitudes y obligatoriedad correctos para cada columna.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe)

    # Act
    schema = schema_gen.generate_schema_dict()

    # Assert
    assert isinstance(schema, dict)
    assert "id_usuario" in schema
    
    # Verificamos los tipos de datos inferidos para Oracle
    assert schema["id_usuario"]["Tipo"] == "NUMBER(1)"
    assert schema["monto_compra"]["Tipo"] == "NUMBER(6, 2)" # 4 enteros + 2 decimales
    assert schema["fecha_registro"]["Tipo"] == "DATE"
    assert schema["pais"]["Tipo"] == "CHAR(2)"
    assert schema["descripcion_larga"]["Tipo"] == "VARCHAR2(20)"
    assert schema["codigo_fijo"]["Tipo"] == "CHAR(5)" # Como todos tienen la misma longitud, debería ser CHAR
    
    # Verificamos la obligatoriedad
    assert schema["id_usuario"]["obligatoria"] == "Obligatorio"
    assert schema["descripcion_larga"]["obligatoria"] == "No Obligatorio"

def test_to_oracle_ddl_generates_correct_statement(optimized_dataframe: pd.DataFrame):
    """
    Verifica que la sentencia DDL CREATE TABLE para Oracle se genere
    con la sintaxis y los tipos de datos correctos.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe)
    table_name = "CLIENTES_PROCESADOS"

    # Act
    ddl = schema_gen.to_oracle_ddl(table_name)

    # Assert
    # Verificamos que contenga las partes clave de una sentencia DDL
    assert ddl.startswith(f"CREATE TABLE {table_name} (")
    assert '"id_usuario" NUMBER(1),' in ddl
    assert '"monto_compra" NUMBER(6, 2),' in ddl
    assert '"fecha_registro" DATE,' in ddl
    assert '"pais" CHAR(2),' in ddl
    assert '"descripcion_larga" VARCHAR2(20),' in ddl
    assert '"codigo_fijo" CHAR(5)' in ddl # El último no debe tener coma
    assert ddl.endswith(");")

def test_to_excel_creates_file(optimized_dataframe: pd.DataFrame, tmp_path: Path):
    """
    Verifica que el método to_excel cree un archivo en la ruta especificada.
    """
    # Arrange
    schema_gen = SchemaGenerator(optimized_dataframe)
    output_path = tmp_path / "schema_test.xlsx"

    # Act
    schema_gen.to_excel(output_path)

    # Assert
    assert output_path.exists()
    assert output_path.is_file()
    
    # Prueba adicional (opcional pero recomendada): leer el archivo y verificar su contenido
    df_from_excel = pd.read_excel(output_path, index_col=0)
    assert "Tipo" in df_from_excel.columns
    assert df_from_excel.loc["id_usuario"]["Tipo"] == "NUMBER(1)"
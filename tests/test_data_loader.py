import pytest
import pandas as pd
from pathlib import Path
from pydantic import ValidationError

from data_profiler import (
    DataLoader,
    LoadConfig,
    CsvLoadConfig,
    ExcelLoadConfig,
    FileType,
    UnsupportedFileTypeError,
    InvalidConfigurationError,
)


@pytest.fixture
def data_dir(tmp_path: Path) -> Path:
    # Arrange: Crear archivo CSV de prueba
    csv_path = tmp_path / "test.csv"
    csv_path.write_text("col1,col2\n1,a\n2,b")

    # Archivo con otro encoding
    csv_latin1_path = tmp_path / "test_latin1.csv"
    csv_latin1_path.write_text("col1;col2\n1;áéíóú", encoding="latin-1")

    # Archivo Excel de prueba
    excel_path = tmp_path / "test.xlsx"
    df1 = pd.DataFrame({"col1": [1, 2], "col2": ["a", "b"]})
    df2 = pd.DataFrame({"col3": [3, 4], "col4": ["c", "d"]})
    with pd.ExcelWriter(excel_path) as writer:
        df1.to_excel(writer, sheet_name="Sheet1", index=False)
        df2.to_excel(writer, sheet_name="Sheet2", index=False)

    return tmp_path


# -- Pruebas para DataLoader --

#* CSV Tests
def test_load_csv_successfully(data_dir: Path):
    file_path = data_dir / "test.csv"
    config = CsvLoadConfig(file_path=file_path, separator=",")
    loader = DataLoader(config)

    df = loader.load()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2
    assert list(df.columns) == ["col1", "col2"]


def test_load_csv_with_different_encoding(data_dir: Path):
    file_path = data_dir / "test_latin1.csv"
    config = CsvLoadConfig(file_path=file_path, separator=";", encoding="latin-1")
    loader = DataLoader(config)

    df = loader.load()

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert list(df.columns) == ["col1", "col2"]
    assert df.iloc[0]["col2"] == "áéíóú"

def test_load_csv_with_incorrect_delimiter_raises_error(data_dir: Path):
    """
    Verifica que se lanza un error cuando el delimitador proporcionado
    no es el correcto para el archivo CSV.
    """
    csv_path = data_dir / "test.csv"
    csv_path.write_text("nombre,edad\nAna,30\nLuis,25")

    # 2. Creamos una configuración INCORRECTA, especificando un punto y coma.
    config = CsvLoadConfig(file_path=csv_path, separator=";")
    loader = DataLoader(config)

    # Act & Assert
    # 3. Verificamos que se lance InvalidConfigurationError
    #    con el mensaje que definiste en tu código.
    with pytest.raises(InvalidConfigurationError, match="El archivo CSV parece tener un solo campo. Verifica el separador."):
        loader.load()
        
#* Excel Tests
def test_load_excel_all_sheets(data_dir: Path):
    file_path = data_dir / "test.xlsx"
    config = ExcelLoadConfig(file_path=file_path)
    loader = DataLoader(config)

    df = loader.load()

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (4, 4)
    assert list(df.columns) == ["col1", "col2", "col3", "col4"]


def test_load_excel_specific_sheets(data_dir: Path):
    file_path = data_dir / "test.xlsx"
    config = ExcelLoadConfig(file_path=file_path, sheet_name="Sheet1")
    loader = DataLoader(config)

    df = loader.load()

    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 2)  # Only rows from Sheet1
    assert list(df.columns) == ["col1", "col2"]

#* Other Tests
def test_file_not_found_error():
    with pytest.raises(ValidationError, match="Path does not point to a file"):
        CsvLoadConfig(file_path="non_existent_file.csv")


def test_invalid_excel_sheet_name(data_dir: Path):
    file_path = data_dir / "test.xlsx"
    config = ExcelLoadConfig(file_path=file_path, sheet_name="NonExistentSheet")
    loader = DataLoader(config)

    with pytest.raises(
        InvalidConfigurationError, match="no existe en el archivo Excel"
    ):
        loader.load()


def test_load_json_successfully(data_dir: Path):
    """
    Verifica que un archivo JSON se carga correctamente.
    """
    # Arrange
    json_path = data_dir / "test.json"
    json_path.write_text('[{"col1": 1, "col2": "a"}, {"col1": 2, "col2": "b"}]')

    # Usamos la configuración base porque no necesita parámetros extra
    config = LoadConfig(file_path=json_path)
    loader = DataLoader(config)

    # Act
    df = loader.load()

    # Assert
    assert len(df) == 2
    assert list(df.columns) == ["col1", "col2"]


def test_load_parquet_successfully(data_dir: Path):
    """
    Verifica que un archivo Parquet se carga correctamente.
    """
    # Arrange
    parquet_path = data_dir / "test.parquet"
    original_df = pd.DataFrame({"colA": [10, 20], "colB": ["x", "y"]})
    original_df.to_parquet(parquet_path)

    config = LoadConfig(file_path=parquet_path)
    loader = DataLoader(config)

    # Act
    df = loader.load()

    # Assert
    assert len(df) == 2
    pd.testing.assert_frame_equal(
        original_df, df
    )  # La mejor forma de comparar DataFrames


def test_unsupported_file_type_raises_error(data_dir: Path):
    """
    Verifica que se lanza un error para tipos de archivo no soportados.
    """
    # Arrange
    unsupported_file = data_dir / "test.zip"
    unsupported_file.touch()  # Solo crea el archivo, no necesita contenido

    # Act & Assert
    # FileType.from_extension lanzará el error
    with pytest.raises(
        UnsupportedFileTypeError, match="Tipo de archivo no soportado: .zip"
    ):
        FileType.from_extension(unsupported_file.suffix)

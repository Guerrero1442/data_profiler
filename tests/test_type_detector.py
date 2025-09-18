import pytest
import pandas as pd
import numpy as np
from pathlib import Path
from data_profiler import (
    TypeDetector,
    TypeDetectorConfig,
)

@pytest.fixture
def detector_config(tmp_path: Path) -> TypeDetectorConfig:
    """Crea una configuración de TypeDetector para las pruebas."""
    yaml_content = """
text_keywords:
  - "codigo"
  - "identificacion"
float_keywords:
  - "valor"
  - "total"
date_formats:
  - "%Y-%m-%d"
  - "%d/%m/%Y"
"""
    config_path = tmp_path / "keywords.yaml"
    config_path.write_text(yaml_content)
    # Usamos valores explícitos para umbrales para que las pruebas sean predecibles
    return TypeDetectorConfig(
        keyword_config_path=config_path,
        cardinality_threshold=0.1, # 10%
        unique_count_limit=50,
        keywords=TypeDetectorConfig.from_yaml(str(config_path)).keywords
    )
    
def test_forced_text_conversion_based_on_keyword(detector_config: TypeDetectorConfig):
    data = {"codigo_producto": [100, 200, 300], "precio": [10.5, 20.0, 15.75]}
    df = pd.DataFrame(data)
    detector = TypeDetector(df, detector_config)
    
    result_df  = detector.run_detection()
    
    assert pd.api.types.is_string_dtype(result_df["codigo_producto"])
    assert pd.api.types.is_float_dtype(result_df["precio"])
    
def test_numeric_conversion_with_comma_decimal(detector_config: TypeDetectorConfig):
    data = {"valor_transaccion": ["1.500,75", "99,50", "1.000,00"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()
    
    # Assert
    assert pd.api.types.is_float_dtype(result_df["valor_transaccion"])
    assert result_df["valor_transaccion"][0] == 1500.75
    
def test_numeric_conversion_with_dot_decimal(detector_config: TypeDetectorConfig):
    data = {"valor_transaccion": ["1,500.75", "99.50", "1,000.00"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()
    
    # Assert
    assert pd.api.types.is_float_dtype(result_df["valor_transaccion"])
    assert result_df["valor_transaccion"][0] == 1500.75
    
def test_numeric_conversion_to_integer(detector_config: TypeDetectorConfig):
    data = {"cantidad": [10.0, 5.0, np.nan, 20.0]}
    df = pd.DataFrame(data)
    detector = TypeDetector(df, detector_config)
    
    result_df = detector.run_detection()
    
    assert pd.api.types.is_integer_dtype(result_df["cantidad"])
    assert str(result_df["cantidad"].dtype) == "Int64"  # Verifica que es Int64 (soporta NaN)
    
def test_categorical_conversion_for_low_cardinality(detector_config: TypeDetectorConfig):
    # Arrange
    # 3 valores únicos en 100 filas. Cardinalidad = 3/100 = 0.03 < 0.1
    data = {"estado": ["activo"] * 40 + ["inactivo"] * 35 + ["pendiente"] * 25}
    df = pd.DataFrame(data)
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert isinstance(result_df["estado"].dtype, pd.CategoricalDtype)
    
def test_no_categorical_conversion_for_high_cardinality(detector_config: TypeDetectorConfig):
    """
    Verifica que las columnas con muchos valores únicos (alta cardinalidad)
    NO se conviertan a 'category'.
    """
    # Arrange
    # 11 valores únicos en 20 filas. Cardinalidad = 11/20 = 0.55 > 0.05
    data = {"id_usuario": [f"id_{i}" for i in range(20)]}
    df = pd.DataFrame(data)
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert not isinstance(result_df["id_usuario"].dtype, pd.CategoricalDtype)
    assert pd.api.types.is_string_dtype(result_df["id_usuario"]) # Debería ser 'string'

def test_mixed_type_column_is_not_converted_to_category(detector_config: TypeDetectorConfig):
    """
    Verifica que una columna con tipos mixtos (números y texto), aunque tenga
    baja cardinalidad, no se convierta a 'category'.
    """
    # Arrange
    # Baja cardinalidad (2 valores únicos), pero son de tipos diferentes.
    data = {"datos_mixtos": [100, "cien", 100, "cien", 100]}
    df = pd.DataFrame(data)
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    # Debería terminar como 'string', no como 'category'.
    assert not isinstance(result_df["datos_mixtos"].dtype, pd.CategoricalDtype)
    assert pd.api.types.is_string_dtype(result_df["datos_mixtos"])

def test_empty_columns_are_converted_to_string(detector_config: TypeDetectorConfig):
    """
    Verifica que una columna que solo contiene valores nulos (NaN, None)
    se convierta a tipo 'string' para evitar problemas.
    """
    # Arrange
    data = {"columna_llena": [1, 2, 3], "columna_vacia": [np.nan, None, np.nan]}
    df = pd.DataFrame(data)
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert pd.api.types.is_string_dtype(result_df["columna_vacia"])
    
def test_date_conversion_with_keyword_and_format(detector_config: TypeDetectorConfig):
    """
    Verifica que las columnas con nombres como 'fecha' y formatos válidos
    se conviertan a datetime.
    """
    # Arrange
    data = {"fecha_ingreso": ["2024-01-15", "2024-03-20", "2024-05-10"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(result_df["fecha_ingreso"])
    
def test_date_conversion_with_mixed_formats(detector_config: TypeDetectorConfig):
    """
    Verifica que la conversión a fecha funcione incluso si hay formatos
    mixtos en la misma columna, siempre que ambos formatos estén en la config.
    """
    # Arrange
    data = {"fecha_evento": ["2024-05-10", "15/06/2024", "2024-07-20"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    # NOTA: pd.to_datetime es inteligente, pero tu bucle actual puede que no.
    # Esta prueba es útil para ver cómo se comporta tu lógica.
    # Con la lógica actual, es posible que falle.
    # Si falla, una posible mejora es intentar pd.to_datetime sin formato
    # si los formatos específicos fallan. Por ahora, veamos qué pasa.
    result_df = detector.run_detection()

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(result_df["fecha_evento"])
    
def test_date_conversion_strips_time_information(detector_config: TypeDetectorConfig):
    """
    Verifica que la información de hora (HH:MM:SS) se ignora
    correctamente durante la conversión a fecha.
    """
    # Arrange
    data = {"ultimo_acceso": ["2024-01-15 10:30:00", "2024-03-20 23:59:59"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(result_df["ultimo_acceso"])
    # Verificamos que el primer elemento es la fecha correcta, con la hora
    assert result_df["ultimo_acceso"][0] == pd.Timestamp("2024-01-15 10:30:00")
    
# test si las horas son solo 00:00:00 no tomarlo como Timestamp sino como fecha
def test_date_conversion_ignores_midnight_time(detector_config: TypeDetectorConfig):
    """
    Verifica que si la hora es siempre '00:00:00', la columna se trate
    como fecha sin hora (date) y no como timestamp.
    """
    # Arrange
    data = {"fecha_nacimiento": ["2024-01-15 00:00:00", "2024-03-20 00:00:00"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(result_df["fecha_nacimiento"])
    # Verificamos que el primer elemento es la fecha correcta, sin la hora
    assert result_df["fecha_nacimiento"][0] == pd.Timestamp("2024-01-15")

def test_date_conversion_with_null_values(detector_config: TypeDetectorConfig):
    """
    Verifica que los valores nulos (None, np.nan) en una columna de fechas
    se manejen correctamente, convirtiéndolos a NaT.
    """
    # Arrange
    data = {"fecha_baja": ["2024-02-01", None, "2024-04-10", np.nan]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert pd.api.types.is_datetime64_any_dtype(result_df["fecha_baja"])
    assert pd.isna(result_df["fecha_baja"][1]) # Verificamos que el nulo se mantuvo
    assert pd.isna(result_df["fecha_baja"][3])
    
def test_column_with_invalid_date_strings_remains_string(detector_config: TypeDetectorConfig):
    """
    Verifica que si una columna contiene strings que no son fechas
    (y no coinciden con ningún formato), la columna no se convierte.
    """
    # Arrange
    data = {"col_con_fecha_mala": ["2024-01-15", "no es una fecha", "2024-13-01"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    # Act
    result_df = detector.run_detection()

    # Assert
    assert not pd.api.types.is_datetime64_any_dtype(result_df["col_con_fecha_mala"])
    assert pd.api.types.is_string_dtype(result_df["col_con_fecha_mala"])
    

def test_numeric_column_with_date_keyword_in_name_is_not_converted_to_date(detector_config: TypeDetectorConfig):
    """
    Verifica que una columna con datos puramente numéricos (como un año o un ID)
    no sea convertida a fecha, aunque su nombre contenga 'fecha'.
    """
    data = {"id_lote_fecha": ["202401", "202402", "202403"]}
    df = pd.DataFrame(data, dtype="object")
    detector = TypeDetector(df, detector_config)

    result_df = detector.run_detection()

    assert not pd.api.types.is_datetime64_any_dtype(result_df["id_lote_fecha"])
    
    assert pd.api.types.is_integer_dtype(result_df["id_lote_fecha"])
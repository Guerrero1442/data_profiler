import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Union

class SchemaGenerator:
    """
    Genera un esquema de base de datos y metadatos a partir de un DataFrame.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Inicializa el generador de esquemas.

        Args:
            df: El DataFrame optimizado del cual se generará el esquema.
        """
        self.df = df
        self.schema_dict: Dict[str, Dict[str, Any]] = {}

    def generate_schema_dict(self) -> Dict[str, Dict[str, Any]]:
        """
        Analiza el DataFrame y genera un diccionario con los metadatos de cada columna.

        Returns:
            Un diccionario que contiene el esquema y los metadatos.
        """
        for column in self.df.columns:
            oracle_type = self._detect_oracle_type(column)
            is_mandatory = 'No Obligatorio' if self.df[column].isnull().any() else 'Obligatorio'
            allowed_values = self._analyze_patterns(column)

            self.schema_dict[column] = {
                'Tipo': oracle_type,
                'longitud': self.df[column].astype(str).str.len().max(),
                'valores_permitidos': allowed_values,
                'obligatoria': is_mandatory,
            }
        return self.schema_dict

    def to_excel(self, output_path: Union[str, Path]) -> None:
        """
        Exporta el diccionario del esquema a un archivo Excel.

        Args:
            output_path: La ruta donde se guardará el archivo Excel.
        """
        if not self.schema_dict:
            self.generate_schema_dict()

        df_schema = pd.DataFrame.from_dict(self.schema_dict, orient='index')
        df_schema.index.name = 'Columna'
        df_schema.to_excel(output_path, index=True)

    def to_oracle_ddl(self, table_name: str) -> str:
        """
        Genera una sentencia DDL CREATE TABLE para Oracle.

        Args:
            table_name: El nombre de la tabla a crear.

        Returns:
            Una cadena de texto con la sentencia DDL.
        """
        if not self.schema_dict:
            self.generate_schema_dict()

        ddl = f"CREATE TABLE {table_name} (\\n"
        for column, metadata in self.schema_dict.items():
            ddl += f'    \"{column}\" {metadata["Tipo"]},\\n'

        ddl = ddl.rstrip(',\\n') + '\\n);'
        return ddl

    def _detect_oracle_type(self, column: str) -> str:
        """
        Detecta el tipo de dato de Oracle más apropiado para una columna.
        """
        dtype = self.df[column].dtype
        if pd.api.types.is_integer_dtype(dtype):
            max_len = self.df[column].astype(str).str.len().max()
            return f"NUMBER({max_len})"
        elif pd.api.types.is_float_dtype(dtype):
            # Asumiendo 2 decimales de precisión
            precision = self._get_numeric_precision(self.df[column])
            return f"NUMBER({precision}, 2)"
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            return "DATE"
        elif isinstance(self.df[column].dtype, pd.CategoricalDtype) or pd.api.types.is_string_dtype(dtype) or pd.api.types.is_object_dtype(dtype):
            max_len = self.df[column].astype(str).str.len().max()
            # Si todos los valores tienen la misma longitud, se puede usar CHAR
            if (self.df[column].str.len() == max_len).all():
                 return f"CHAR({int(max_len)})"
            return f"VARCHAR2({int(max_len)})"
        else:
            return "VARCHAR2(255)"

    def _get_numeric_precision(self, series: pd.Series) -> int:
        """Calcula la precisión para un campo numérico."""
        series_int = series.dropna().astype(int)
        max_int_len = series_int.astype(str).str.len().max()
        return max_int_len + 2  # 2 para los decimales

    def _analyze_patterns(self, column: str) -> Union[str, List[Any]]:
        """
        Analiza los patrones de una columna para obtener valores permitidos o estadísticas.
        """
        dtype = self.df[column].dtype
        if isinstance(self.df[column].dtype, pd.CategoricalDtype):
            return self.df[column].cat.categories.tolist()
        elif pd.api.types.is_numeric_dtype(dtype):
            stats = self.df[column].describe()
            return (f"min: {stats['min']:.1f} - max: {stats['max']:.1f} - "
                    f"mean: {stats['mean']:.1f} - std: {stats['std']:.1f}")
        elif pd.api.types.is_datetime64_any_dtype(dtype) and self.df[column].notnull().any():
            min_date = self.df[column].min().strftime('%d/%m/%Y')
            max_date = self.df[column].max().strftime('%d/%m/%Y')
            return f"min: {min_date} - max: {max_date}"
        else:
            return ''
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Union
from loguru import logger
from .dialects import Dialect

class SchemaGenerator:
    """
    Genera un esquema de base de datos y metadatos a partir de un DataFrame.
    """

    def __init__(self, df: pd.DataFrame, dialect: Dialect):
        """
        Inicializa el generador de esquemas.

        Args:
            df: El DataFrame optimizado del cual se generará el esquema.
        """
        self.df = df
        self.dialect = dialect
        self.schema_dict: Dict[str, Dict[str, Any]] = {}

    def generate_schema_dict(self) -> Dict[str, Dict[str, Any]]:
        """
        Analiza el DataFrame y genera un diccionario con los metadatos de cada columna.

        Returns:
            Un diccionario que contiene el esquema y los metadatos.
        """
        for column in self.df.columns:
            db_type = self.dialect.map_dtype(self.df[column])
            is_mandatory = 'No Obligatorio' if self.df[column].isnull().any() else 'Obligatorio'
            allowed_values = self._analyze_patterns(column)
            if self.df[column].isnull().all():
                longitud = 0
            else:
                longitud = self.df[column].astype(str).str.len().max()

            self.schema_dict[column] = {
                'tipo': db_type,
                'longitud': longitud,
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

    def to_ddl_file(self, table_name: str, output_path: Union[str, Path]) -> None:
        """
        Genera una sentencia DDL CREATE TABLE para Oracle.

        Args:
            table_name: El nombre de la tabla a crear.

        Returns:
            Una cadena de texto con la sentencia DDL.
        """
        if not self.schema_dict:
            self.generate_schema_dict()
            
        ddl = self.dialect.generate_ddl(table_name, self.schema_dict)
    
        output_path = Path(output_path)
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(ddl)
            
        logger.info(f"DDL guardado en {output_path}")
        
    def _analyze_patterns(self, column: str) -> Union[str, List[Any]]:
        """
        Analiza los patrones de una columna para obtener valores permitidos o estadísticas.
        """
        dtype = self.df[column].dtype
        if isinstance(self.df[column].dtype, pd.CategoricalDtype) :
            return self.df[column].cat.categories.tolist()
        elif pd.api.types.is_bool_dtype(dtype):
            return self.df[column].dropna().unique().tolist()
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
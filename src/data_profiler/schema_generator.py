import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Union
from loguru import logger
from .dialects import Dialect
from .context import TypeDetectorConfig

class SchemaGenerator:
    """
    Generates a database schema and metadata from a DataFrame.
    """

    def __init__(self, df: pd.DataFrame, dialect: Dialect, config: TypeDetectorConfig):
        """
        Initializes the SchemaGenerator
        
        Args:
            df: The DataFrame to analyze.
            dialect: The database dialect to use for type mapping and DDL generation.
        """
        self.df = df
        self.dialect = dialect
        self.schema_dict: Dict[str, Dict[str, Any]] = {}
        self.config = config

    def generate_schema_dict(self) -> Dict[str, Dict[str, Any]]:
        """
        Analyzes the DataFrame and generates a schema dictionary with metadata.
        
        Returns: 
            A dictionary where keys are column names and values are dictionaries with metadata.
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
                'observaciones': self.observaciones_column(column)
            }
        return self.schema_dict
    
    def observaciones_column(self, column: str) -> str:   
        
        # Verificar si es recomendable convertir a booleano
        recomended_true = self.config.keywords.get("recomended_true_values", [])
        recomended_false = self.config.keywords.get("recomended_false_values", [])
        
        if recomended_true or recomended_false:
            # Obtener valores únicos no nulos de la columna
            unique_values = self.df[column].dropna().astype(str).str.lower().unique()
            
            allowed_recomended_values = set([str(v).lower() for v in recomended_true + recomended_false])
                        
            if (set(unique_values).issubset(allowed_recomended_values) and len(unique_values) <= 2 and len(unique_values) > 0) or column.lower().startswith("is"):
                return "Es recomendable transformar esta columna a Booleano."
             
        # revisar si la columna es nula
        if self.df[column].isnull().all():
            return "Columna vacía."
        
        # Verificar si fue forzada como texto por palabra clave
        keywords_text = self.config.keywords.get("text_keywords", [])
        
        if any(keyword in column.lower() for keyword in keywords_text):
            return "Columna de texto forzada por palabra clave."
        
        return ""

    def to_excel(self, output_path: Union[str, Path]) -> None:
        """
        Exports the schema dictionary to an Excel file.
        
        Args:
            output_path: The path where the Excel file will be saved.
        """
        if not self.schema_dict:
            self.generate_schema_dict()

        df_schema = pd.DataFrame.from_dict(self.schema_dict, orient='index')
        df_schema.index.name = 'Columna'
        df_schema.to_excel(output_path, index=True)

    def to_ddl_file(self, table_name: str, output_path: Union[str, Path]) -> None:
        """
        Generates a DDL file for creating a table with the schema.
        
        Args:
            table_name: The name of the table to create.
            output_path: The path where the DDL file will be saved.
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
        Analyzes the patterns of a column to obtain allowed values or statistics.
        
        Args:
            column: The column name to analyze.
            
        Returns:
            A list of allowed values for categorical/boolean columns, or a string with statistics for numeric/datetime columns.
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
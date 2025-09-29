from typing import Any, Dict, Optional
import pandas as pd
import pyarrow as pa
from .base import Dialect
from loguru import logger

class BigQueryDialect(Dialect):
    
    def __init__(self, project_id: Optional[str] = None, dataset_id: Optional[str] = None):
        self.project_id = project_id
        self.dataset_id = dataset_id
    
    def map_dtype(self, series: pd.Series) -> str:
        dtype = series.dtype
        
        if pd.api.types.is_integer_dtype(dtype):
            return "INTEGER"
        elif pd.api.types.is_float_dtype(dtype):
            return "NUMERIC"
        elif isinstance(dtype, pd.CategoricalDtype) or pd.api.types.is_string_dtype(dtype):
            return "STRING"
        elif pd.api.types.is_bool_dtype(dtype):
            return "BOOLEAN"
        elif pa.types.is_date(dtype.pyarrow_dtype):
            return "DATE"
        elif pa.types.is_timestamp(dtype.pyarrow_dtype):
            return "TIMESTAMP"
        else:
            logger.warning(f"Could not determine type for {series.name}. Defaulting to STRING.")
            return "STRING"
        
    def generate_ddl(self, table_name: str, schema: Dict[str, Dict[str, Any]]) -> str:
        if self.project_id and self.dataset_id:
            full_table_name = f"`{self.project_id}.{self.dataset_id}.{table_name}`"
        elif self.dataset_id:
            full_table_name = f"`{self.dataset_id}.{table_name}`"
        else:
            full_table_name = f"`{table_name}`"
            
        columns_definitions = []
        
        for column, metadata in schema.items():
            column_name = f"`{column}`"
            columns_definitions.append(f'    {column_name} {metadata["tipo"]}')
            
        columns_str = ",\n".join(columns_definitions)
        
        return f"""-- Esquema generado para la tabla {table_name} (BigQuery)
CREATE OR REPLACE TABLE {full_table_name} (
{columns_str}
);
"""
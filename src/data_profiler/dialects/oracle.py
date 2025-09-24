import pandas as pd
import pyarrow as pa
from .base import Dialect
from loguru import logger

class OracleDialect(Dialect):
    
    def _get_numeric_precision(self, series: pd.Series) -> int:
        """Calcula la precisión para un campo numérico."""
        series_int = series.dropna().astype(int)
        max_int_len = series_int.astype(str).str.len().max()
        return max_int_len + 2  # 2 para los decimales
    
    def map_dtype(self, series: pd.Series) -> str:
        dtype = series.dtype
        
        if series.isnull().all():
            return "VARCHAR2(1)"
        
        if dtype.name == 'string' and series.dropna().eq("").all():
            return "VARCHAR2(1)"
        
        if pd.api.types.is_integer_dtype(dtype):
            max_len = series.astype(str).str.len().max() 
            return f"NUMBER({max_len})"
        elif pd.api.types.is_float_dtype(dtype):
            precision = self._get_numeric_precision(series)
            return f"NUMBER({precision}, 2)"
        elif isinstance(dtype, pd.CategoricalDtype) or pd.api.types.is_string_dtype(dtype):
            max_len = series.astype(str).str.len().max()
            if (series.str.len() == max_len).all():
                 return f"CHAR({int(max_len)})"
            else:
                return f"VARCHAR2({int(max_len)})"
        elif pa.types.is_date(dtype.pyarrow_dtype):
            return "DATE"
        elif pa.types.is_timestamp(dtype.pyarrow_dtype):
            return "TIMESTAMP"
        else:
            logger.warning(f"No se pudo determinar el tipo de {series.name}. Usando VARCHAR2(255) por defecto.")
            return "VARCHAR2(255)"
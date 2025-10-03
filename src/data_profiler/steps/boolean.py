import pandas as pd
import pyarrow as pa
from loguru import logger
from .base import ConversionStep

class BooleanConversionStep(ConversionStep):
    
    def _is_potential_boolean(self, series: pd.Series) -> bool:
        """Check if a pandas Series can be converted to boolean."""
        
        unique_values = series.dropna().unique()
        if len(unique_values) != 2:
            return False

        true_values = self.config.keywords.get("boolean_true_values", ["true", "yes", "si"])
        false_values = self.config.keywords.get("boolean_false_values", ["false", "no"])
        
        allowed_values = set(true_values + false_values)
        
        unique_values_lower = {str(val).strip().lower() for val in unique_values}
        
        return unique_values_lower.issubset(allowed_values)
    
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Starting boolean conversion step.")
        
        true_values = self.config.keywords.get("boolean_true_values", ["true", "yes", "si"])
        false_values = self.config.keywords.get("boolean_false_values", ["false", "no"])
        
        map_to_bool = {str(v).lower(): True for v in true_values}
        map_to_bool.update({str(v).lower(): False for v in false_values})
        
        
        for col in df.select_dtypes(include=['object', 'string']).columns:
            if self._is_potential_boolean(df[col]):
                logger.debug(f"Converting column '{col}' to boolean.")
                try:
                    df[col] = df[col].astype(str).str.strip().str.lower().map(map_to_bool).astype('boolean[pyarrow]')
                    logger.success(f"Column '{col}' successfully converted to boolean.")
                except Exception as e:
                    logger.warning(f"Error converting column '{col}' to boolean: {e}")
        
        return df
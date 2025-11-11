import pandas as pd

from .context import TypeDetectorConfig
from loguru import logger
from .steps import (
    EmptyColumnsToStringStep,
    ForcedTextConversionStep,
    NumericConversionStep,
    DateConversionStep,
    BooleanConversionStep,
    CategoricalConversionStep,
    ObjectToStringStep
)

#! utilizar pandas con pyarrow
class TypeDetector:
    """
    Detects and converts data types of DataFrame columns based on a series of processing steps.
    """
    def __init__(self, df: pd.DataFrame, config: TypeDetectorConfig):
        """
        Initializes the TypeDetector with a DataFrame and configuration.
        
        Args:
            df (pd.DataFrame): The input DataFrame to process.
            config (TypeDetectorConfig): Configuration for type detection.
        
        """
        self.df = df.copy()
        self.config = config

        self.steps = [
            EmptyColumnsToStringStep(config),
            NumericConversionStep(config),
            DateConversionStep(config),
            BooleanConversionStep(config),
            CategoricalConversionStep(config),
            ForcedTextConversionStep(config),
            ObjectToStringStep(config)
        ]
        
    def run_detection(self) -> pd.DataFrame:
        """
        Runs the entire type detection process by executing each step in sequence.
        
        Returns:
            pd.DataFrame: The DataFrame with detected and converted data types.
        """
        df_processed = self.df
        
        for step in self.steps:
            logger.debug(f"Ejecutando paso: {step.__class__.__name__}")
            df_processed = step.process(df_processed)
            
        logger.info("Detección de tipos completada.")
        logger.success(f"Tipos de datos finales:\n{df_processed.dtypes}")
        
        return df_processed
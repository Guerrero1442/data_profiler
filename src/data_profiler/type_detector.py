import pandas as pd
from .context import TypeDetectorConfig
from loguru import logger
from .steps import (
    EmptyColumnsToStringStep,
    ForcedTextConversionStep,
    NumericConversionStep,
    DateConversionStep,
    CategoricalConversionStep,
    ObjectToStringStep
)

#! utilizar pandas con pyarrow
class TypeDetector:
    def __init__(self, df: pd.DataFrame, config: TypeDetectorConfig):
        self.df = df.copy()
        self.config = config

        self.steps = [
            EmptyColumnsToStringStep(config),
            ForcedTextConversionStep(config),
            NumericConversionStep(config),
            DateConversionStep(config),
            CategoricalConversionStep(config),
            ObjectToStringStep(config)
        ]
        
    def run_detection(self) -> pd.DataFrame:

        df_processed = self.df
        
        for step in self.steps:
            logger.info(f"Ejecutando paso: {step.__class__.__name__}")
            df_processed = step.process(df_processed)
            
        logger.info("Detección de tipos completada.")
        logger.success(f"Tipos de datos finales:\n{df_processed.dtypes}")
        
        return df_processed
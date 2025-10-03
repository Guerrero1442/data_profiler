import pandas as pd
import pyarrow as pa
from loguru import logger
from .base import ConversionStep


class ForcedTextConversionStep(ConversionStep):
    def _is_forced_text(self, col_name: str) -> bool:
        return any(
            keyword in col_name.lower()
            for keyword in self.config.keywords.get("text_keywords", [])
        )

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Iniciando conversión forzada a texto por palabra clave.")
        for col in df.columns:
            if self._is_forced_text(col):
                logger.success(f"Columna '{col}' forzada a 'string' por palabra clave.")
                df[col] = df[col].astype("string[pyarrow]")

        return df


class EmptyColumnsToStringStep(ConversionStep):
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        empty_mask = df.isnull().all()
        empty_cols = df.columns[empty_mask].tolist()
        if empty_cols:
            logger.warning(f"Columnas vacías detectadas: {empty_cols}")
            for col in empty_cols:
                df[col] = df[col].astype("string[pyarrow]")
            logger.success(
                f"Columnas vacías convertidas a 'string[pyarrow]': {empty_cols}"
            )
        return df


class ObjectToStringStep(ConversionStep):
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.debug("Convirtiendo columnas 'object' restantes a 'string'.")
        for col in df.select_dtypes(include=["object"]).columns:
            df[col] = df[col].astype("string[pyarrow]")
            logger.success(f"Columna '{col}' convertida a 'string[pyarrow]'.")
        return df

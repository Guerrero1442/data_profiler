import pandas as pd
import pyarrow as pa
from loguru import logger
from .base import ConversionStep


class CategoricalConversionStep(ConversionStep):
    def _has_mixed_types(self, series: pd.Series) -> bool:
        """Detecta si una serie tiene tipos mixtos (números y strings)."""
        # Obtener valores únicos no nulos
        unique_values = series.dropna().unique()

        if len(unique_values) == 0:
            return False

        # Verificar tipos de los valores únicos
        types_found = set()
        for value in unique_values:
            if isinstance(value, (int, float)) and not pd.isna(value):
                types_found.add("numeric")
            elif isinstance(value, str):
                # Verificar si el string representa un número
                try:
                    float(value)
                    types_found.add("numeric")
                except (ValueError, TypeError):
                    types_found.add("string")
            else:
                types_found.add("other")

        # Si hay más de un tipo, son tipos mixtos
        return len(types_found) > 1

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando conversion de columnas categoricas.")
        for col in df.select_dtypes(include=["object", "string"]).columns:
            num_unique = df[col].nunique(dropna=True)
            total_count = len(df[col])
            cardinality = num_unique / total_count if total_count > 0 else 0
            if (
                cardinality < self.config.cardinality_threshold
                and num_unique <= self.config.unique_count_limit
                and num_unique > 0
            ):
                if self._has_mixed_types(df[col]):
                    logger.warning(
                        f"Columna '{col}' tiene tipos mixtos. No se convertira a 'category'."
                    )
                else:
                    logger.debug(
                        f"Columna '{col}' cumple con umbral de cardinalidad ({cardinality:.4f} < {self.config.cardinality_threshold}) y unicos ({num_unique} <= {self.config.unique_count_limit})."
                    )
                    df[col] = df[col].astype('category')
                    logger.success(
                        f"Columna '{col}' convertida a 'category' (Cardinalidad: {cardinality:.4f}, Unicos: {num_unique})"
                    )
            else:
                logger.debug(
                    f"Columna '{col}' no cumple con umbral de cardinalidad ({cardinality:.4f} >= {self.config.cardinality_threshold}) o unicos ({num_unique} > {self.config.unique_count_limit})."
                )

        logger.info("Conversion de columnas categoricas completada.")
        return df

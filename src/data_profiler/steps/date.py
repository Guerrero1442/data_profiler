import pandas as pd
from loguru import logger
from .base import ConversionStep


class DateConversionStep(ConversionStep):
    def _all_times_are_midnight(self, datetime_series: pd.Series) -> bool:
        """
        Verifica si todos los valores no nulos de una serie datetime tienen hora 00:00:00
        """
        if datetime_series.empty:
            return False

        # Filtrar valores no nulos
        valid_dates = datetime_series.dropna()

        if len(valid_dates) == 0:
            return False

        # Verificar si todas las horas son 00:00:00
        all_midnight = all(
            dt.hour == 0 and dt.minute == 0 and dt.second == 0 for dt in valid_dates
        )

        return all_midnight

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando conversion de columnas de fecha.")
        for col in df.select_dtypes(include=["object"]).columns:
            # Condición para identificar columnas que podrían ser fechas
            is_date_like = (
                df[col].str.contains(r"[/-]", na=False).any()
                or "fecha" in col.lower()
                or "date" in col.lower()
            )

            if is_date_like:
                original_col = df[col].copy()
                try:
                    mask_invalid = original_col.str.contains("0001", na=False)
                    original_col.loc[mask_invalid] = pd.NaT

                    # 2. Intentamos que pandas infiera el formato automáticamente.
                    #    Esto es muy potente y a menudo resuelve formatos mixtos.
                    converted_series = pd.to_datetime(original_col, errors="coerce")

                    if converted_series.notna().sum() >= (
                        original_col.notna().sum() / 2
                    ):
                        # Verificar si todas las horas son medianoche
                        if self._all_times_are_midnight(converted_series):
                            converted_series = converted_series.dt.normalize()

                        df[col] = converted_series
                        logger.success(
                            f"Columna '{col}' convertida a 'datetime' (formato inferido)."
                        )
                    else:
                        # Si la inferencia no fue exitosa, volvemos a la columna original y no hacemos nada.
                        df[col] = original_col

                except (TypeError, ValueError):
                    # Si CUALQUIER cosa falla, restauramos la columna y seguimos.
                    df[col] = original_col

        return df

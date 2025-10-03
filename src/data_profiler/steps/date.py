import pandas as pd
import pyarrow as pa
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
        logger.debug("Iniciando conversion de columnas de fecha.")
        for col in df.select_dtypes(include=["object", "datetime64[ns]"]).columns:
            
            is_date_like = False
            # Condición para identificar columnas que podrían ser fechas
            if pd.api.types.is_string_dtype(df[col].dtype):
                is_date_like = (
                    df[col].str.contains(r"[/-]", na=False).any()
                    or "fecha" in col.lower()
                    or "date" in col.lower()
                )

            if is_date_like or pd.api.types.is_datetime64_ns_dtype(df[col].dtype):
                original_col = df[col].copy()
                try:
                    if df[col].dtype == "object":
                        mask_invalid = original_col.str.contains("0001", na=False)
                        original_col.loc[mask_invalid] = pd.NaT

                        # 2. Intentamos que pandas infiera el formato automáticamente.
                        #    Esto es muy potente y a menudo resuelve formatos mixtos.
                        converted_series = pd.to_datetime(original_col, errors="coerce")
                    else:
                        converted_series = original_col

                    if converted_series.notna().sum() >= (
                        original_col.notna().sum() / 2
                    ):
                        # Verificar si todas las horas son medianoche
                        if self._all_times_are_midnight(converted_series):
                            pandas_date_arrow_type = pd.ArrowDtype(pa.date32())
                            df[col] = converted_series.astype(pandas_date_arrow_type)
                        else:
                            pandas_timestamp_arrow_type = pd.ArrowDtype(
                                pa.timestamp("s")
                            )
                            df[col] = converted_series.astype(
                                pandas_timestamp_arrow_type
                            )
                        logger.success(
                            f"Columna '{col}' convertida exitosamente a tipo fecha/timestamp."
                        )
                    else:
                        # Si la inferencia no fue exitosa, volvemos a la columna original y no hacemos nada.
                        df[col] = original_col

                except (TypeError, ValueError):
                    # Si CUALQUIER cosa falla, restauramos la columna y seguimos.
                    df[col] = original_col

        return df

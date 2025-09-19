import pandas as pd
from loguru import logger
from .base import ConversionStep


class NumericConversionStep(ConversionStep):
    def _infer_decimal_separator(self, series: pd.Series) -> str:
        sample = series.dropna().astype(str).head(1000)
        comma = sample.str.contains(",").sum()
        dot = sample.str.contains(r"\.").sum()
        return "," if comma > dot else "."

    def _convert_float_columns_to_int(self, df: pd.DataFrame) -> None:
        logger.info(
            "Convirtiendo columnas de tipo 'float' a 'Int64' cuando sea posible (no tengan decimales)."
        )
        for col in df.select_dtypes(include=["float64"]).columns:
            logger.debug(
                f"Revisando columna '{col}' para posible conversion a 'Int64'."
            )
            if ((df[col] % 1 == 0.0) | (df[col].isnull())).all():
                df[col] = df[col].astype("Int64")
                logger.success(f"Columna '{col}' convertida a 'Int64'.")
            else:
                logger.debug(
                    f"Columna '{col}' no puede ser convertida a 'Int64' (tiene decimales)."
                )

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando conversion de columnas numericas.")
        for col in df.select_dtypes(include=["object"]).columns:
            decimal_sep = self._infer_decimal_separator(df[col])

            try:
                if decimal_sep == ",":
                    df[col] = (
                        df[col]
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False)
                        .pipe(pd.to_numeric, errors="raise")
                    )
                    logger.success(
                        f"Columna '{col}' convertida a 'float' con separador decimal ','"
                    )
                else:
                    df[col] = (
                        df[col]
                        .str.replace(",", "", regex=False)
                        .pipe(pd.to_numeric, errors="raise")
                    )

                    logger.success(
                        f"Columna '{col}' convertida a 'float' con separador decimal '.'"
                    )

                # Redondear a 2 decimales solo si tiene decimales
                if (df[col] % 1 != 0).any():
                    df[col] = df[col].round(2)

            except (ValueError, TypeError):
                continue

        self._convert_float_columns_to_int(df)

        return df

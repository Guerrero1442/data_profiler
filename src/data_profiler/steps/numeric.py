import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc  # Usamos el alias 'pc' por convención
from loguru import logger
from .base import ConversionStep


class NumericConversionStep(ConversionStep):
    def _infer_decimal_separator(self, series: pd.Series) -> tuple[str, str]:
        sample = series.dropna().astype(str).head(1000)
        comma = sample.str.contains(",").sum()
        dot = sample.str.contains(r"\.").sum()
        # Si hay más comas que puntos, asumimos que la coma es el separador decimal solo si hay puntos
        if comma > dot:
            return ",", "."
        else:
            return ".", ","

    def _convert_float_columns_to_int(self, df: pd.DataFrame) -> None:
        logger.info(
            "Convirtiendo columnas de tipo 'float' a 'Int64' cuando sea posible (no tengan decimales)."
        )

        float_arrow_cols = [
            col
            for col in df.columns
            if isinstance(df[col].dtype, pd.ArrowDtype)
            and pa.types.is_float64(df[col].dtype.pyarrow_dtype)
        ]

        for col in float_arrow_cols:
            logger.debug(
                f"Revisando columna '{col}' para posible conversion a 'int64[pyarrow]'."
            )
            # Redondear a 2 decimales solo si tiene decimales significativos
            # Usar PyArrow compute para verificar decimales
            pa_array = pa.array(df[col])
            # Verificar si hay decimales usando floor
            floored = pc.floor(pa_array)
            has_decimals = pc.equal(pa_array, floored)


            if (pc.all(has_decimals).as_py()) | (df[col].isnull()).all():
                df[col] = df[col].astype(pd.ArrowDtype(pa.int64()))
                logger.success(f"Columna '{col}' convertida a 'int64[pyarrow]'.")
            else:
                logger.debug(
                    f"Columna '{col}' no puede ser convertida a 'int64[pyarrow]' (tiene decimales)."
                )

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Iniciando conversion de columnas numericas.")
        for col in df.select_dtypes(include=["object", "float64"]).columns:

            try:
                if df[col].dtype == "object":
                    decimal_sep, thousand_sep = self._infer_decimal_separator(df[col])

                    array_temp = pc.replace_substring(
                    pa.array(df[col].astype(str)), pattern=thousand_sep, replacement=""
                )

                    array_limpio = pc.replace_substring(
                        array_temp, pattern=decimal_sep, replacement="."
                    )

                    df[col] = array_limpio.cast(pa.float64())
                    
                df[col] = df[col].astype(pd.ArrowDtype(pa.float64()))
                
                logger.debug(f"Columna '{col}' convertida a 'float64[pyarrow]'. {df[col].dtype}")

                # Redondear a 2 decimales solo si tiene decimales significativos
                # Usar PyArrow compute para verificar decimales
                pa_array = pa.array(df[col])
                # Verificar si hay decimales usando floor
                floored = pc.floor(pa_array)
                has_decimals = pc.not_equal(pa_array, floored)
                
                if pc.any(has_decimals).as_py():
                    df[col] = df[col].round(2)

            except (ValueError, TypeError) as e:
                logger.debug(
                    f"Columna '{col}' no pudo ser convertida a 'float64[pyarrow]'. Se mantiene su tipo original: {e}"
                )
                continue

        self._convert_float_columns_to_int(df)

        return df

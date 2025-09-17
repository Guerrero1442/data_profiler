import pandas as pd
from .context import TypeDetectorConfig
from loguru import logger

#! utilizar pandas con pyarrow
class TypeDetector:
    def __init__(self, df: pd.DataFrame, config: TypeDetectorConfig):
        self.df = df.copy()
        self.config = config

    def run_detection(self) -> pd.DataFrame:
        """Ejecuta la deteccion de tipos y categorias en el DataFrame."""
        logger.info("Iniciando deteccion de tipos y categorias.")
        self._identify_empty_columns()
        self._convert_forced_text_columns()
        self._convert_numeric_columns()
        self._convert_float_columns_to_int()
        self._convert_date_columns()
        self._convert_categorical_columns()
        self._convert_object_columns_to_string()
        logger.info("Deteccion de tipos y categorias completada.")
        
        # visualizar tipos
        logger.debug(f"Tipos de datos finales:\n{self.df.dtypes}")
        
        return self.df

    def _identify_empty_columns(self):
        empty_mask = self.df.isnull().all()
        empty_cols = self.df.columns[empty_mask].tolist()
        if empty_cols:
            logger.warning(f"Columnas vacias detectadas: {empty_cols}")
            for col in empty_cols:
                self.df[col] = pd.Series(dtype="string[pyarrow]")

            logger.success(f"Columnas vacias convertidas a tipo 'string[pyarrow]': {empty_cols}")

    def _is_forced_text(self, col_name: str) -> bool:
        return any(
            keyword in col_name.lower()
            for keyword in self.config.keywords.get("text_keywords", [])
        )

    def _is_forced_float(self, col_name: str) -> bool:
        return any(
            keyword in col_name.lower()
            for keyword in self.config.keywords.get("float_keywords", [])
        )

    def _convert_forced_text_columns(self):
        logger.info("Iniciando conversion de columnas forzadas a texto.")
        for col in self.df.columns:
            if self._is_forced_text(col):
                logger.success(f"Columna '{col}' forzada a 'string' por palabra clave.")
                self.df[col] = self.df[col].astype("string[pyarrow]")

    def _infer_decimal_separator(self, series: pd.Series) -> str:
        sample = series.dropna().astype(str).head(1000)
        comma = sample.str.contains(",").sum()
        dot = sample.str.contains(r"\.").sum()
        return "," if comma > dot else "."

    def _convert_numeric_columns(self):
        logger.info("Iniciando conversion de columnas numericas.")
        for col in self.df.select_dtypes(include=["object"]).columns:
            decimal_sep = self._infer_decimal_separator(self.df[col])

            try:
                if decimal_sep == ",":
                    self.df[col] = (
                        self.df[col]
                        .str.replace(".", "", regex=False)
                        .str.replace(",", ".", regex=False)
                    )
                    self.df[col] = pd.to_numeric(self.df[col], errors="raise")
                    self.df[col] = self.df[col].round(2)
                    logger.success(
                        f"Columna '{col}' convertida a 'float' con separador decimal ','"
                    )
                else:
                    self.df[col] = self.df[col].str.replace(",", "", regex=False)
                    self.df[col] = pd.to_numeric(self.df[col], errors="raise")
                    self.df[col] = self.df[col].round(2)
                    logger.success(
                        f"Columna '{col}' convertida a 'float' con separador decimal '.'"
                    )
                
            except (ValueError, TypeError) as e:
                logger.debug(
                    f"Columna '{col}' no pudo ser convertida a 'float'. Se mantiene como 'object'.: {e}"
                )
                continue
            
    def _convert_float_columns_to_int(self):
        logger.info("Convirtiendo columnas de tipo 'float' a 'Int64' cuando sea posible (no tengan decimales).")
        for col in self.df.select_dtypes(include=["float64"]).columns:
            logger.debug(f"Revisando columna '{col}' para posible conversion a 'Int64'.")
            if ((self.df[col] % 1 == 0.0 )| (self.df[col].isnull())).all():
                self.df[col] = self.df[col].astype("Int64")
                logger.success(f"Columna '{col}' convertida a 'Int64'.")
            else:
                logger.debug(f"Columna '{col}' tiene decimales. No se convierte a 'Int64'.")

    def _convert_date_columns(self):
        logger.info("Iniciando conversion de columnas de fecha.")
        for col in self.df.select_dtypes(include=["object"]).columns:
            # Condición para identificar columnas que podrían ser fechas
            is_date_like = (
                self.df[col].str.contains(r"[/-]", na=False).any()
                or "fecha" in col.lower()
                or "date" in col.lower()
            )

            if is_date_like:
                original_col = self.df[col].copy() 
                try:
                    temp_series = self.df[col].str.slice(0, 10)
                    mask_invalid = temp_series.str.contains("0001", na=False)
                    temp_series.loc[mask_invalid] = pd.NaT

                    # 2. Intentamos que pandas infiera el formato automáticamente.
                    #    Esto es muy potente y a menudo resuelve formatos mixtos.
                    converted_series = pd.to_datetime(temp_series, errors='coerce')

                    if converted_series.notna().sum() >= (original_col.notna().sum() / 2):
                        self.df[col] = converted_series
                        logger.success(
                            f"Columna '{col}' convertida a 'datetime' (formato inferido)."
                        )
                    else:
                        # Si la inferencia no fue exitosa, volvemos a la columna original y no hacemos nada.
                        self.df[col] = original_col
                        logger.debug(
                            f"La inferencia de fecha para '{col}' no fue concluyente. Se mantiene como object."
                        )

                except Exception as e:
                    # Si CUALQUIER cosa falla, restauramos la columna y seguimos.
                    self.df[col] = original_col
                    logger.debug(
                        f"Columna '{col}' no pudo ser convertida a 'datetime' por un error inesperado: {e}"
                    )
                
                
    def _convert_categorical_columns(self):
        logger.info("Iniciando conversion de columnas categoricas.")
        for col in self.df.select_dtypes(include=["object", "string"]).columns:
            num_unique = self.df[col].nunique(dropna=True)
            total_count = len(self.df[col])
            cardinality = num_unique / total_count if total_count > 0 else 0
            if (
                cardinality < self.config.cardinality_threshold 
                and num_unique <= self.config.unique_count_limit
                and num_unique > 0
            ):
                if self._has_mixed_types(self.df[col]):
                    logger.warning(
                        f"Columna '{col}' tiene tipos mixtos. No se convertira a 'category'."
                    )
                else:
                    logger.debug(
                        f"Columna '{col}' cumple con umbral de cardinalidad ({cardinality:.4f} < {self.config.cardinality_threshold}) y unicos ({num_unique} <= {self.config.unique_count_limit})."
                    )
                    self.df[col] = self.df[col].astype("category")
                    logger.success(
                        f"Columna '{col}' convertida a 'category' (Cardinalidad: {cardinality:.4f}, Unicos: {num_unique})"
                    )
            else:
                logger.debug(
                    f"Columna '{col}' no cumple con umbral de cardinalidad ({cardinality:.4f} >= {self.config.cardinality_threshold}) o unicos ({num_unique} > {self.config.unique_count_limit})."
                )

    def _convert_object_columns_to_string(self):
        logger.info("Convirtiendo columnas de tipo 'object' a 'string[pyarrow]'.")
        for col in self.df.select_dtypes(include=["object"]).columns:
            self.df[col] = self.df[col].astype("string[pyarrow]")
            logger.success(f"Columna '{col}' convertida a 'string[pyarrow]'.")
            
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
                types_found.add('numeric')
            elif isinstance(value, str):
                # Verificar si el string representa un número
                try:
                    float(value)
                    types_found.add('numeric')
                except (ValueError, TypeError):
                    types_found.add('string')
            else:
                types_found.add('other')
        
        # Si hay más de un tipo, son tipos mixtos
        return len(types_found) > 1
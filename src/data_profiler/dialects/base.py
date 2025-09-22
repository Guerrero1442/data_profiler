import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, Any


class Dialect(ABC):
    """
    Clase base abstracta para definir dialectos de bases de datos.
    """

    @abstractmethod
    def map_dtype(self, pandas_dtype: Any) -> str:
        """
        Mapea el dtype de una Serie de pandas al tipo de dato de la base de datos.

        Args:
            series: La columna del DataFrame a analizar.

        Returns:
            Un string con el tipo de dato SQL correspondiente.
        """
        pass

    def generate_ddl(self, table_name: str, schema: Dict[str, Dict[str, Any]]) -> str:
        """
        Genera la sentencia DDL CREATE TABLE completa.

        Args:
            table_name: El nombre de la tabla.
            schema: El diccionario de esquema generado.

        Returns:
            Un string con la sentencia DDL completa.
        """
        columns_definitions = []

        for column, metadata in schema.items():
            columns_definitions.append(f'    "{column}" {metadata["tipo"]}')

        columns_str = ",\n".join(columns_definitions)

        return f"""-- Esquema generado para la tabla {table_name}
CREATE TABLE {table_name} (
        {columns_str}
    );
    """

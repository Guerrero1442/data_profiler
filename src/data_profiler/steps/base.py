# src/data_profiler/steps/base.py

import pandas as pd
from abc import ABC, abstractmethod
from data_profiler import TypeDetectorConfig


class ConversionStep(ABC):
    """
    Clase base abstracta para un paso en el proceso de detección de tipos.
    """

    def __init__(self, config: TypeDetectorConfig):
        self.config = config

    @abstractmethod
    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Ejecuta la lógica de conversión de este paso.

        Args:
            df (pd.DataFrame): El DataFrame de entrada.

        Returns:
            pd.DataFrame: El DataFrame procesado.
        """
        pass

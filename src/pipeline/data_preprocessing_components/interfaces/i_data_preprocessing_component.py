from abc import ABC, abstractmethod

from src.entities.pipeline.component_result import DataPreprocessingResult


class IDataPreprocessingComponent(ABC):
    @abstractmethod
    def preprocess_data(self) -> DataPreprocessingResult:
        """
        :return: Датакласс с предобработанными данными
        """

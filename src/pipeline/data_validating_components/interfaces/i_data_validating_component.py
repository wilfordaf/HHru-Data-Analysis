from abc import ABC, abstractmethod

from src.entities.pipeline.component_result import DataValidatingResult


class IDataValidatingComponent(ABC):
    @abstractmethod
    def validate_data(self) -> DataValidatingResult:
        """
        :return: Датакласс с предобработанными данными
        """

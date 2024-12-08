from abc import ABC, abstractmethod

from src.entities.pipeline.component_result import DataExtractingResult


class IDataExtractingComponent(ABC):
    @abstractmethod
    def get_data(self) -> DataExtractingResult:
        """
        :return: Датакласс с данными, полученными из источника
        """

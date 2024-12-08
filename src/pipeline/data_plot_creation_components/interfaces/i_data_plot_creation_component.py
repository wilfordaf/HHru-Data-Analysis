from abc import ABC, abstractmethod

from src.entities.pipeline.component_result import DataPlotCreationResult


class IDataPlotCreationComponent(ABC):
    @abstractmethod
    def create_plots(self) -> DataPlotCreationResult:
        """
        :return: Результат создания графиков.
        """

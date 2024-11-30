from abc import ABC, abstractmethod
from typing import Any, Dict

from plotly.graph_objects import Figure


class ILogger(ABC):
    """
    Публикует данные в целевую среду.
    Актуальные цели:
    - ClearML
    - Консоль (локально)
    """

    @abstractmethod
    def publish_dictionary_values(self, name: str, data_to_publish: Dict[str, Any]) -> None:
        """
        Публикует дискретные значения.
        Например: параметры шага, метрики...

        :param data_to_publish: Словарь с данными для публикации.
        """

    @abstractmethod
    def publish_plots(self, plots_to_publish: Dict[str, Figure]) -> None:
        """
        Публикует графики.
        Например: train-val loss...

        :param plots_to_publish: Словарь с данными для публикации.
        """

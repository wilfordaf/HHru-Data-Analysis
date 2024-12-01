from abc import ABC, abstractmethod
from typing import Union

from src.entities.pipeline import PipelineConfiguration


class IConfigLoader(ABC):
    @abstractmethod
    def get_config(self) -> Union[PipelineConfiguration]:
        """
        :return: Объект конфигурации
        """

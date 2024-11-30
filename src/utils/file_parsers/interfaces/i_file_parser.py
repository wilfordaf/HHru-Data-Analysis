from abc import ABC, abstractmethod
from typing import Any, Dict


class IFileParser(ABC):
    @abstractmethod
    def retrieve_data(self) -> Dict[str, Any]:
        """
        :return: Словарь ключ-значение, полученный из файла.
        """

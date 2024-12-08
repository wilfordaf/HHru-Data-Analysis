from abc import ABC, abstractmethod
from typing import Any, Dict


class IDataValidator(ABC):
    @abstractmethod
    def validate_data(self) -> Dict[str, Any]:
        """
        :return: Результат валидации данных
        """

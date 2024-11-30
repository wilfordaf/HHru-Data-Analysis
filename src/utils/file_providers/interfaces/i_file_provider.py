from abc import ABC, abstractmethod
from pathlib import Path


class IFileProvider(ABC):
    @abstractmethod
    def provide_file(self, original_path: Path, target_path: Path) -> Path:
        """
        :param original_path: Оригинальный путь до файла / директории.
        :param target_path: Целевой путь до файла / директории.
        :return: Путь до файла / директории, подготовленного поставщиком.
        """

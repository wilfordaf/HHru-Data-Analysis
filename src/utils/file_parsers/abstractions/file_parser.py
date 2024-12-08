import os.path
from pathlib import Path
from typing import Any, Dict

from src.utils.exceptions import ServiceError
from src.utils.file_parsers.interfaces import IFileParser


class FileParser(IFileParser):
    _EXTENSION = ".stub"

    def __init__(self, filepath: Path):
        if filepath.suffix != self._EXTENSION:
            raise ServiceError(f"Расширение {filepath.suffix} не является {self._EXTENSION}")

        if not os.path.exists(filepath):
            raise ServiceError(f"Файл по пути {filepath.resolve()} не найден")

        self._filepath = filepath

    def retrieve_data(self) -> Dict[str, Any]:
        try:
            return self._parse_file()
        except Exception as e:
            raise ServiceError(f"При чтении файла {self._filepath} возникла ошибка: {str(e)}") from e

    def _parse_file(self) -> Dict[str, Any]:
        raise NotImplementedError()

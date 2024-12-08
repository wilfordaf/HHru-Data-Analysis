import json
from typing import Any, Dict

from src.utils.file_parsers.abstractions import FileParser


class JsonFileParser(FileParser):
    _EXTENSION = ".json"

    def _parse_file(self) -> Dict[str, Any]:
        with open(self._filepath, encoding="utf-8") as fin:
            content: Dict[str, Any] = json.load(fin)
            return content

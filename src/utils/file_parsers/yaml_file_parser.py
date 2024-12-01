from typing import Any, Dict

from yaml import load
from yaml.loader import SafeLoader

from src.utils.file_parsers.abstractions import FileParser


class YamlFileParser(FileParser):
    _EXTENSION = ".yaml"

    def _parse_file(self) -> Dict[str, Any]:
        with open(self._filepath, encoding="utf-8") as fin:
            data: Dict[str, Any] = load(fin, Loader=SafeLoader)
            return data

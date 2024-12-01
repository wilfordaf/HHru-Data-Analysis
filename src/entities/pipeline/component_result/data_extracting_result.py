from typing import Dict

from pydantic.dataclasses import dataclass

from src.enums import DatasetName


@dataclass(config={"arbitrary_types_allowed": True})
class DataExtractingResult:
    result: Dict[str, DatasetName]

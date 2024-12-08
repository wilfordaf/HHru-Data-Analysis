from typing import List

from pydantic import BaseModel


class ExtractionStepProperties(BaseModel):
    positions_to_extract: List[str]

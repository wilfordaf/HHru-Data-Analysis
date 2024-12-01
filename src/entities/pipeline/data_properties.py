from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict

from src.enums import DatasetTag


class DataProperties(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    name: str
    description: str
    tag: DatasetTag
    custom_properties: Optional[Dict[str, Any]] = None

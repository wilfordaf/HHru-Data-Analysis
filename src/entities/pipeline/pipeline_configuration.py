from typing import Dict

from pydantic import BaseModel, ConfigDict

from src.entities.pipeline.common_properties import CommonProperties
from src.entities.pipeline.components import Components
from src.entities.pipeline.data_properties import DataProperties
from src.enums import DatasetName


class PipelineConfiguration(BaseModel):
    model_config = ConfigDict(use_enum_values=True)

    common_properties: CommonProperties
    components: Components
    dataset: Dict[DatasetName, DataProperties]

from typing import Optional

from pydantic import BaseModel, ConfigDict

from src.entities.pipeline.component_properties import (
    DataPlotCreationStepProperties,
    DataValidatingStepProperties,
    ExtractionStepProperties,
    PreprocessingStepProperties,
)


class Components(BaseModel):
    model_config = ConfigDict(protected_namespaces=())

    extraction_step_properties: Optional[ExtractionStepProperties] = None
    preprocessing_step_properties: Optional[PreprocessingStepProperties] = None
    data_validating_step_properties: Optional[DataValidatingStepProperties] = None
    data_plot_creation_step_properties: Optional[DataPlotCreationStepProperties] = None

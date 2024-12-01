from src.data_controlling.interfaces import IDataController
from src.entities.pipeline import PipelineConfiguration
from src.entities.pipeline.component_result import (
    DataExtractingResult,
    DataPlotCreationResult,
    DataPreprocessingResult,
    DataValidatingResult,
)
from src.utils.artifact_publication.interfaces.i_logger import ILogger


def extraction_step(
    config: PipelineConfiguration,
    data_controller: IDataController,
    target_logger: ILogger,
) -> DataExtractingResult:
    from src.pipeline.data_extracting_components import DataExtractingComponent

    return DataExtractingComponent(
        config,
        data_controller,
        target_logger,
    ).get_data()


def preprocessing_step(
    config: PipelineConfiguration,
    data_controller: IDataController,
    extracting_result: DataExtractingResult,
    target_logger: ILogger,
) -> DataPreprocessingResult:
    from src.pipeline.data_preprocessing_components import DataPreprocessingComponent

    return DataPreprocessingComponent(
        config,
        data_controller,
        extracting_result,
        target_logger,
    ).preprocess_data()


def data_validating_step(
    config: PipelineConfiguration,
    data_controller: IDataController,
    preprocessing_result: DataPreprocessingResult,
    target_logger: ILogger,
) -> DataValidatingResult:
    from src.pipeline.data_validating_components import DataValidatingComponent

    return DataValidatingComponent(
        config,
        data_controller,
        preprocessing_result,
        target_logger,
    ).validate_data()


def data_plot_creation_step(
    config: PipelineConfiguration,
    data_controller: IDataController,
    preprocessing_result: DataPreprocessingResult,
    target_logger: ILogger,
) -> DataPlotCreationResult:
    from src.pipeline.data_plot_creation_component import DataPlotCreationComponent

    return DataPlotCreationComponent(
        config,
        data_controller,
        preprocessing_result,
        target_logger,
    ).create_plots()

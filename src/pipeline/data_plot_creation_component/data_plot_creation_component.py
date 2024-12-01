import pandas as pd
import plotly.graph_objects as go

from src.data_controlling.interfaces import IDataController
from src.entities.pipeline import PipelineConfiguration
from src.entities.pipeline.component_result import DataPlotCreationResult, DataPreprocessingResult
from src.pipeline.data_plot_creation_component.interfaces import IDataPlotCreationComponent
from src.utils.artifact_publication.interfaces import ILogger
from utils.exceptions.service_error import ServiceError


class DataPlotCreationComponent(IDataPlotCreationComponent):
    def __init__(
        self,
        config: PipelineConfiguration,
        data_controller: IDataController,
        preprocessing_result: DataPreprocessingResult,
        target_logger: ILogger,
    ):
        self._config = config
        self._preprocessing_result = preprocessing_result
        self._data_controller = data_controller
        self._target_logger = target_logger

    def create_plots(self) -> DataPlotCreationResult:
        step_parameters = self._config.components.data_plot_creation_step_properties
        if step_parameters is None:
            raise ServiceError("Пустые параметры шага загрузки данных")

        dataset_name = self._preprocessing_result.result["preprocessed_data"]
        dataset_parameters = self._data_controller.get_dataset_parameters(dataset_name).custom_properties
        if dataset_parameters is None:
            raise ServiceError(f"Обнаружены пустые параметры датасета {dataset_name}")

        dataset = self._data_controller.get_dataset(dataset_name)

        df = pd.DataFrame(dataset)
        age_histogram = go.Figure(
            data=[go.Histogram(x=df["Возраст"], nbinsx=10)],
            layout_title_text="Распределение возраста",
        )

        scatter_plot = go.Figure(
            data=[go.Scatter(x=df["Возраст"], y=df["ЗП"], mode="markers")],
            layout_title_text="Возраст vs Зарплата",
        )

        plots = {"age_histogram": age_histogram, "scatter_plot": scatter_plot}
        self._target_logger.publish_plots(plots)

        summary_data = {"Mean Age": df["Возраст"].mean(), "Mean Salary": df["ЗП"].mean(), "Total Entries": len(df)}
        self._target_logger.publish_dictionary_values("Dataset Summary", summary_data)

        return DataPlotCreationResult(success=True)

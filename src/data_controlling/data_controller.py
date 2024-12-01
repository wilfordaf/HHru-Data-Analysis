from pathlib import Path

import pandas as pd

from src.data_controlling.interfaces import IDataController
from src.entities.pipeline import DataProperties, PipelineConfiguration
from src.enums import DatasetName
from src.utils.exceptions import ServiceError
from src.utils.file_managers.interfaces import IFileManager


class DataController(IDataController):
    _PROJECT_ROOT_PATH = Path(__file__).parents[1].resolve()
    _DATASET_EXTRACTING_DATE_COLUMN_NAME = "pipeline_load_date"

    def __init__(
        self,
        config: PipelineConfiguration,
        file_manager: IFileManager,
    ):
        self._config = config
        self._file_manager = file_manager

        self._file_manager.provide_artifacts_to_project_dir = config.common_properties.provide_artifacts_to_project_dir

    @property
    def project_root(self) -> Path:
        return self._PROJECT_ROOT_PATH

    @property
    def dataset_extracting_date_column_name(self) -> str:
        return self._DATASET_EXTRACTING_DATE_COLUMN_NAME

    def get_dataset(self, dataset_name: DatasetName) -> pd.DataFrame:
        dataset_parameters = self.get_dataset_parameters(dataset_name)
        return self._file_manager.load_dataset(dataset_parameters)

    def save_dataset(self, dataset: pd.DataFrame, dataset_name: DatasetName) -> None:
        dataset_parameters = self.get_dataset_parameters(dataset_name)
        self._file_manager.save_dataset(dataset, dataset_parameters)

    def get_dataset_parameters(self, dataset_name: DatasetName) -> DataProperties:
        dataset_parameters = self._config.dataset.get(dataset_name.value)
        if dataset_parameters is None:
            raise ServiceError(f"Не найдены параметры датасета с именем {dataset_name}")

        return dataset_parameters

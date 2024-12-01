import os
from pathlib import Path

import pandas as pd

from src.entities.pipeline import DataProperties
from src.utils.file_managers.interfaces import IFileManager
from src.utils.file_providers import LocalFileProvider


class FileManager(IFileManager):
    _PROJECT_ROOT = Path(__file__).parents[3].resolve()
    _DATASET_SOURCES_DIR = _PROJECT_ROOT.parent / "datasets"
    _MODEL_SOURCES_DIR = _PROJECT_ROOT.parent / "model_sources"

    def __init__(self):
        self._local_file_provider = LocalFileProvider()

    @property
    def provide_artifacts_to_project_dir(self) -> bool:
        return self._provide_artifacts_to_project_dir

    @provide_artifacts_to_project_dir.setter
    def provide_artifacts_to_project_dir(self, value: bool) -> None:
        self._provide_artifacts_to_project_dir = value

    def load_dataset(self, dataset_properties: DataProperties) -> pd.DataFrame:
        raise NotImplementedError()

    def save_dataset(self, dataset: pd.DataFrame, dataset_properties: DataProperties) -> None:
        raise NotImplementedError()

    @staticmethod
    def _save_dataset_on_disk(dataset: pd.DataFrame, dataset_path: Path, dataset_properties: DataProperties) -> None:
        save_parameters = {}
        if dataset_properties.custom_properties is not None:
            save_parameters = dataset_properties.custom_properties.get("save_parameters", {})

        dataset.to_csv(dataset_path, index=False, **save_parameters)

    @staticmethod
    def _load_dataset_from_disk(dataset_path: Path, dataset_properties: DataProperties) -> pd.DataFrame:
        load_parameters = {}
        if dataset_properties.custom_properties is not None:
            load_parameters = dataset_properties.custom_properties.get("load_parameters", {})

        if not (os.path.isfile(dataset_path) or os.path.isdir(dataset_path)):
            raise FileNotFoundError(f"Датасет не найден по пути {dataset_path}. Рабочий каталог {os.getcwd()}")

        df: pd.DataFrame = pd.read_csv(dataset_path, **load_parameters)  # type: ignore
        return df

    def _get_local_dataset_path(self, dataset_properties: DataProperties) -> Path:
        dataset_name = f"{dataset_properties.name}.csv"
        dataset_tag = str(dataset_properties.tag)
        return Path(self._DATASET_SOURCES_DIR / dataset_tag / dataset_name).resolve()

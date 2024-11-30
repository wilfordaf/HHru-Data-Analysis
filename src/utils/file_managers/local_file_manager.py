import os

import pandas as pd

from src.entities.pipeline import DataProperties
from src.utils.exceptions import ServiceError
from src.utils.file_managers import FileManager


class LocalFileManager(FileManager):
    _METRICS_FILEPATH = "metrics.json"

    def __init__(self):
        super().__init__()

    def load_dataset(self, dataset_properties: DataProperties) -> pd.DataFrame:
        dataset_name = dataset_properties.name
        try:
            dataset_path = self._get_local_dataset_path(dataset_properties)
            dataset = self._load_dataset_from_disk(dataset_path, dataset_properties)
        except FileNotFoundError as fe:
            raise fe
        except Exception as e:
            raise ServiceError(f"Не удалось загрузить датасет {dataset_name}:\n{e}")

        return dataset

    def save_dataset(self, dataset: pd.DataFrame, dataset_properties: DataProperties) -> None:
        dataset_name = dataset_properties.name
        dataset_path = self._get_local_dataset_path(dataset_properties)
        if not os.path.isdir(dataset_path.parents[0]):
            raise ServiceError(f"Директория не найдена в {dataset_path}")
        try:
            self._save_dataset_on_disk(
                dataset=dataset,
                dataset_path=dataset_path,
                dataset_properties=dataset_properties,
            )
        except Exception as e:
            raise ServiceError(f"Не удалось сохранить датасет {dataset_name}:\n{e}")

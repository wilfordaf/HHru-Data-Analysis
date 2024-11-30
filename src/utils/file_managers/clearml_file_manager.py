import os
import tempfile
from pathlib import Path

import pandas as pd
from clearml import Dataset

from src import logger
from src.entities.pipeline import DataProperties
from src.utils.exceptions import ServiceError
from src.utils.file_managers import FileManager


class ClearMLFileManager(FileManager):
    def __init__(self, project_name: str, provide_artifacts_to_project_dir: bool = False):
        super().__init__()
        self._project_name = project_name
        self._provide_artifacts_to_project_dir = provide_artifacts_to_project_dir

    def load_dataset(self, dataset_properties: DataProperties) -> pd.DataFrame:
        dataset_name = dataset_properties.name
        logger.debug(f"Выполняется загрузка датасета {dataset_name} с сервера ClearML")
        try:
            clearml_dataset = Dataset.get(
                dataset_project=self._project_name,
                dataset_name=dataset_name,
                dataset_tags=[dataset_properties.tag],
            )
            local_dataset_folder = clearml_dataset.get_local_copy()

            clearml_dataset_path = Path(os.path.join(local_dataset_folder, os.listdir(local_dataset_folder)[0]))
            clearml_dataset_path = clearml_dataset_path.resolve()

            if self._provide_artifacts_to_project_dir:
                dataset_path = self._get_local_dataset_path(dataset_properties)
                self._local_file_provider.provide_file(clearml_dataset_path, dataset_path)
            else:
                dataset_path = clearml_dataset_path
        except ValueError as ve:
            raise FileNotFoundError(f"Не удалось получить датасет с сервера ClearML: {str(ve)}") from ve
        except Exception as e:
            raise ServiceError(f"Не удалось загрузить датасет {dataset_name} с сервера ClearML:\n{e}")

        return self._load_dataset_from_disk(dataset_path, dataset_properties)

    def save_dataset(self, dataset: pd.DataFrame, dataset_properties: DataProperties) -> None:
        dataset_name = dataset_properties.name
        dataset_file_name = f"{dataset_name}.csv"
        logger.debug(f"Выполняется сохранение датасета {dataset_name} на сервере ClearML")

        with tempfile.TemporaryDirectory() as temp_dir_name:
            try:
                self._save_dataset_on_disk(
                    dataset=dataset,
                    dataset_path=Path(temp_dir_name) / dataset_file_name,
                    dataset_properties=dataset_properties,
                )

                clearml_dataset = Dataset.create(
                    dataset_name=dataset_name,
                    dataset_project=self._project_name,
                    description=dataset_properties.description,
                    dataset_tags=[str(dataset_properties.tag)],
                )
                clearml_dataset.add_files(path=temp_dir_name)
                clearml_dataset.upload()
                clearml_dataset.finalize()
                clearml_dataset.publish()
            except Exception as e:
                raise ServiceError(f"Не удалось сохранить датасет {dataset_name} на сервере ClearML:\n{e}")

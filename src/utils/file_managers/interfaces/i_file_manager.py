from abc import ABC, abstractmethod

import pandas as pd

from src.entities.pipeline import DataProperties


class IFileManager(ABC):
    @property
    @abstractmethod
    def provide_artifacts_to_project_dir(self) -> bool:
        """
        Метод реализован для создания соответствующего setter
        :return: Значение поля provide_artifacts_to_project_dir
        """

    @provide_artifacts_to_project_dir.setter
    @abstractmethod
    def provide_artifacts_to_project_dir(self, value: bool) -> None:
        """
        Метод реализован для перезаписи поля provide_artifacts_to_project_dir из конфигурации пайплайна.
        :param value: Новое значение параметра provide_artifacts_to_project_dir, которое нужно установить.
        """

    @abstractmethod
    def load_dataset(self, dataset_properties: DataProperties) -> pd.DataFrame:
        """ """

    @abstractmethod
    def save_dataset(self, dataset: pd.DataFrame, dataset_properties: DataProperties) -> None:
        """ """

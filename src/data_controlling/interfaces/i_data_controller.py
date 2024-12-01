from abc import ABC, abstractmethod
from pathlib import Path

import pandas as pd

from src.entities.pipeline.data_properties import DataProperties
from src.enums import DatasetName


class IDataController(ABC):
    @property
    @abstractmethod
    def project_root(self) -> Path:
        """
        Возвращает путь до корневой директории проекта
        :return: полный путь
        """

    @property
    @abstractmethod
    def dataset_extracting_date_column_name(self) -> str:
        """
        Возвращает название колонки с датой выгрузки
        :return: строка, название колонки
        """

    @abstractmethod
    def get_dataset(self, dataset_name: DatasetName) -> pd.DataFrame:
        """
        Возвращает датафрейм, полученный из базы данных, с соответствующим набором параметров

        :param dataset_name: имя датасета, определенное в конфигурации
        :return: датафрейм
        """

    @abstractmethod
    def save_dataset(self, dataset: pd.DataFrame, dataset_name: DatasetName) -> None:
        """
        :param dataset:
        :param dataset_name:
        """

    @abstractmethod
    def get_dataset_parameters(self, dataset_name: DatasetName) -> DataProperties:
        """
        Получает параметры датасета из конфигурации.
        :param dataset_name: Название датасета в системе.
        :return: Объект конфигурации датасета.
        """

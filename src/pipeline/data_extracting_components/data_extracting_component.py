from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd

from src import logger
from src.data_controlling.interfaces import IDataController
from src.entities.pipeline import PipelineConfiguration
from src.entities.pipeline.component_result import DataExtractingResult
from src.enums import DatasetName
from src.pipeline.data_extracting_components.get_data_from_superjob import (
    get_data_from_resume_by_url,
    get_resume_urls_from_page,
)
from src.pipeline.data_extracting_components.interfaces import IDataExtractingComponent
from src.utils.artifact_publication.interfaces import ILogger
from src.utils.exceptions.service_error import ServiceError


class DataExtractingComponent(IDataExtractingComponent):
    _SUPERJOB_BASE_URL = "https://russia.superjob.ru/resume/search_resume.html"
    _PAGES_COUNT = 10
    _DATE_COLUMN_NAME = "Дата обновления резюме"

    def __init__(
        self,
        config: PipelineConfiguration,
        data_controller: IDataController,
        target_logger: ILogger,
    ):
        self._config = config
        self._data_controller = data_controller
        self._target_logger = target_logger

        self._extracted_old_data: Optional[pd.DataFrame] = None

    def get_data(self) -> DataExtractingResult:
        step_parameters = self._config.components.extraction_step_properties
        if step_parameters is None:
            raise ServiceError("Пустые параметры шага загрузки данных")

        dataset_parameters = self._data_controller.get_dataset_parameters(DatasetName.SOURCE_DATA).custom_properties
        if dataset_parameters is None:
            raise ServiceError(f"Обнаружены пустые параметры датасета {DatasetName.SOURCE_DATA}")

        current_date = datetime.now()
        extraction_parameters = self._get_extraction_parameters(dataset_parameters)

        position_dataframes = [
            self._extract_api_data(
                position,
                current_date,
                extraction_parameters,
            )
            for position in step_parameters.positions_to_extract
        ]
        if self._extracted_old_data is not None:
            position_dataframes.append(self._extracted_old_data)

        extracted_data = pd.concat(position_dataframes, ignore_index=True)

        logger.debug(f"Выгружено {extracted_data.shape[0]} строк")

        self._data_controller.save_dataset(extracted_data, DatasetName.SOURCE_DATA)
        logger.info(f"Шаг извлечения данных {DatasetName.SOURCE_DATA} выполнен с параметрами: {dataset_parameters}")

        return DataExtractingResult({"source_data": DatasetName.SOURCE_DATA})

    def _extract_api_data(
        self,
        position: str,
        extraction_date: datetime,
        extraction_parameters: Dict[str, Any],
    ) -> pd.DataFrame:
        extraction_date_column = self._data_controller.dataset_extracting_date_column_name
        position_url = f"{self._SUPERJOB_BASE_URL}?keywords[0][keys]={position}&sbmit=1"
        extract_from = extraction_parameters.get("last_load_date", datetime(year=1970, month=1, day=1))

        dataset: Dict[str, List[str]] = {}
        for page in range(self._PAGES_COUNT):
            position_url_paged = f"{position_url}&page={page}"
            resume_urls = get_resume_urls_from_page(position_url_paged)

            for resume_url in resume_urls:
                info = get_data_from_resume_by_url(resume_url)
                if info["Дата обновления резюме"] < extract_from:
                    return pd.DataFrame(dataset)

                info["Ссылка на резюме"] = resume_url
                info["Искомая позиция"] = position
                info["Дата обновления резюме"] = info["Дата обновления резюме"].date()
                info[extraction_date_column] = str(extraction_date.date())

                for key in info:
                    if key not in dataset:
                        dataset[key] = []

                    dataset[key].append(info[key])

        return pd.DataFrame(dataset)

    def _get_extraction_parameters(self, dataset_parameters: Dict[str, Any]) -> Dict[str, Any]:
        if not dataset_parameters["use_increment"]:
            return {}

        try:
            date_column = self._data_controller.dataset_extracting_date_column_name

            self._extracted_old_data = self._data_controller.get_dataset(DatasetName.SOURCE_DATA)
            self._extracted_old_data[date_column] = self._extracted_old_data[date_column].apply(pd.to_datetime)

            return {"last_load_date": self._extracted_old_data[date_column].max()}
        except Exception as e:
            logger.error(f"Произошла ошибка при загрузке исторических данных: {e}")
            return {}

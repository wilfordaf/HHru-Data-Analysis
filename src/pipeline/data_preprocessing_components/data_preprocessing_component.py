import re
from datetime import datetime
from typing import Any, Dict, Optional, Union

import numpy as np
import pandas as pd
import torch
from pandas._libs.missing import NAType
from transformers import pipeline

from src import logger
from src.data_controlling.interfaces import IDataController
from src.entities.pipeline import PipelineConfiguration
from src.entities.pipeline.component_properties import PreprocessingStepProperties
from src.entities.pipeline.component_result import DataExtractingResult, DataPreprocessingResult
from src.enums import DatasetName
from src.pipeline.data_preprocessing_components.interfaces import IDataPreprocessingComponent
from src.utils.artifact_publication.interfaces import ILogger
from src.utils.exceptions import ServiceError


class DataPreprocessingComponent(IDataPreprocessingComponent):
    def __init__(
        self,
        config: PipelineConfiguration,
        data_controller: IDataController,
        extracting_result: DataExtractingResult,
        target_logger: ILogger,
    ):
        self._config = config
        self._data_controller = data_controller
        self._extracting_result = extracting_result
        self._target_logger = target_logger

        self._jobs_classifier_pipeline: Optional[pipeline] = None

    def preprocess_data(self) -> DataPreprocessingResult:
        step_parameters = self._config.components.preprocessing_step_properties
        if step_parameters is None:
            raise ServiceError("Пустые параметры шага загрузки данных")

        dataset_name = self._extracting_result.result["source_data"]
        dataset_parameters = self._data_controller.get_dataset_parameters(dataset_name).custom_properties
        if dataset_parameters is None:
            raise ServiceError(f"Обнаружены пустые параметры датасета {dataset_name}")

        final_dataset_name = DatasetName.PREPROCESSED_DATA
        final_dataset_parameters = self._data_controller.get_dataset_parameters(final_dataset_name).custom_properties
        if final_dataset_parameters is None:
            raise ServiceError(f"Обнаружены пустые параметры датасета {final_dataset_name}")

        target_data = self._data_controller.get_dataset(dataset_name)

        # TODO: Cache ;)
        final_dataset_parameters["apply_preprocessing_only_to_increment"] = True
        if final_dataset_parameters["apply_preprocessing_only_to_increment"]:
            target_data = self._get_only_increment(target_data)

        logger.debug(f"Предобрабатывается текст {target_data.shape[0]} записей")
        preprocessed_data = self._preprocess_data(target_data, step_parameters)

        if final_dataset_parameters["apply_preprocessing_only_to_increment"]:
            preprocessed_data = self._format_preprocessed_data(preprocessed_data)

        logger.debug(f"Полученный объём предобработанных данных {target_data.shape[0]} записей")
        self._data_controller.save_dataset(preprocessed_data, DatasetName.PREPROCESSED_DATA)

        logger.info(f"Шаг подготовки данных выполнен с параметрами: {step_parameters}")

        return DataPreprocessingResult({"preprocessed_data": DatasetName.PREPROCESSED_DATA})  # type: ignore

    def _preprocess_data(
        self,
        dataset: pd.DataFrame,
        step_parameters: PreprocessingStepProperties,
    ) -> pd.DataFrame:
        if dataset.shape[0] == 0:
            return pd.DataFrame()

        dataset["ЗП"] = dataset["ЗП"].apply(self._extract_salary)
        dataset["Возраст"] = dataset["Возраст"].apply(self._extract_age)
        dataset = self._column_fillna_random(dataset, "Возраст")

        self._init_model()
        dataset = dataset[
            dataset.apply(
                self._clear_unmatching_jobs,
                axis=1,
                args=(step_parameters.unmatching_jobs_threshold,),
            )
        ]
        dataset.drop(columns=["Желаемая должность"], inplace=True)

        return dataset

    def _get_only_increment(self, target_data: pd.DataFrame) -> pd.DataFrame:
        extracting_column = self._data_controller.dataset_extracting_date_column_name
        df: pd.DataFrame = target_data[target_data[extracting_column] == str(datetime.now().date)]
        return df

    def _format_preprocessed_data(self, preprocessed_data: pd.DataFrame) -> pd.DataFrame:
        preprocessed_old_data = self._data_controller.get_dataset(DatasetName.PREPROCESSED_DATA)
        return pd.concat([preprocessed_old_data, preprocessed_data])

    def _format_extracted_data(
        self,
        extracted_data: pd.DataFrame,
        dataset_parameters: Dict[str, Any],
    ) -> pd.DataFrame:
        if dataset_parameters["check_duplicates_by_increment"]:
            extracted_data.drop_duplicates(keep="last", subset=dataset_parameters["increment_on"], inplace=True)
            extracted_data.reset_index(drop=True, inplace=True)
            logger.debug(f"Объём данных после форматирования: {extracted_data.shape[0]} строк")

        return extracted_data

    def _init_model(self) -> None:
        self._jobs_classifier_pipeline = pipeline(
            "zero-shot-classification",
            model="MoritzLaurer/deberta-v3-xsmall-zeroshot-v1.1-all-33",
            device="cuda" if torch.cuda.is_available() else "cpu",
        )

    def _clear_unmatching_jobs(self, row: pd.Series, threshold: float) -> bool:  # type: ignore
        if self._jobs_classifier_pipeline is None:
            raise ServiceError("Classifier is not initialized!")

        result = self._jobs_classifier_pipeline(
            row["Желаемая должность"],
            candidate_labels=[row["Искомая позиция"]],
            multi_label=False,
        )
        score = result["scores"][0]
        comparison_result: bool = score >= threshold
        return comparison_result

    def _extract_age(self, age: str) -> Union[int, NAType]:
        try:
            return int(re.findall(r"\d{2} г", age)[0].split()[0])
        except Exception:
            return pd.NA

    def _extract_salary(self, salary: str) -> Union[int, NAType]:
        try:
            return int(re.sub(r"[^\d]", "", salary))
        except Exception:
            return pd.NA

    def _column_fillna_random(self, dataframe: pd.DataFrame, column: str) -> pd.DataFrame:
        mean = dataframe[column].mean()
        std = dataframe[column].std()

        lower_bound = mean - 3 * std
        upper_bound = mean + 3 * std

        lower_bound = lower_bound if lower_bound >= 0 else 0.0

        random_values = np.random.uniform(lower_bound, upper_bound, size=dataframe[column].isna().sum())
        random_values = random_values.astype(np.int64)
        dataframe.loc[dataframe[column].isna(), column] = random_values

        return dataframe

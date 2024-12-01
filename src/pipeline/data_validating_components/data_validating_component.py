from typing import Any, Callable, Dict

import pandas as pd

from src import logger
from src.data_controlling.interfaces import IDataController
from src.entities.pipeline import PipelineConfiguration
from src.entities.pipeline.component_result import DataPreprocessingResult, DataValidatingResult
from src.enums import DatasetName, DatasetValidationError
from src.pipeline.data_validating_components.component_sources import CustomMetricsValidator, GreatExpectationsValidator
from src.pipeline.data_validating_components.interfaces import IDataValidatingComponent
from src.utils.artifact_publication.interfaces import ILogger
from src.utils.exceptions import ServiceError


class DataValidatingComponent(IDataValidatingComponent):
    def __init__(
        self,
        config: PipelineConfiguration,
        data_controller: IDataController,
        preprocessing_result: DataPreprocessingResult,
        target_logger: ILogger,
    ):
        self._config = config
        self._data_controller = data_controller
        self._preprocessing_result = preprocessing_result
        self._target_logger = target_logger

    def validate_data(self) -> DataValidatingResult:
        dataset_name = self._preprocessing_result.result["preprocessed_data"]
        parameters = self._data_controller.get_dataset_parameters(dataset_name).custom_properties
        if parameters is None:
            raise ServiceError("При валидации модели обнаружены пустые параметры")

        dataset = self._data_controller.get_dataset(dataset_name)

        custom_metrics = {
            "minimal_data": lambda d, p: d.shape[0] >= p["minimal_data_rows"],
            "columns_constraint": lambda d, p: p["target_column"] in d.columns,
            "numeric_instance": lambda d, _: (all([isinstance(salary, int) for salary in d["ЗП"]])),
        }

        custom_validator = CustomMetricsValidator(dataset, parameters, custom_metrics)
        custom_validation_result = custom_validator.validate_data()
        if not custom_validation_result["success"]:
            return DataValidatingResult(  # type: ignore
                success=False,
                error=DatasetValidationError.BAD_DATA_QUALITY_ERROR,
                message=custom_validation_result["message"],
            )

        verified_dataset = self._data_controller.get_dataset(DatasetName.VERIFIED_DATA)

        gx_target_validator = GreatExpectationsValidator(verified_dataset, dataset, parameters)
        validation_result = gx_target_validator.validate_data()
        if not validation_result["success"]:
            return DataValidatingResult(  # type: ignore
                success=False,
                error=validation_result["error"],
                message=validation_result["message"],
            )

        logger.info(f"Шаг валидации данных выполнен с параметрами: {parameters}")
        return DataValidatingResult()

    def _validate_custom_metrics(
        self,
        data: pd.DataFrame,
        parameters: Dict[str, Any],
        custom_metrics: Dict[str, Callable[[pd.DataFrame, Dict[str, Any]], bool]],
    ) -> Dict[str, Any]:
        try:
            metric_results = {metric_name: metric(data, parameters) for metric_name, metric in custom_metrics.items()}
        except Exception as e:
            return {"success": False, "message": f"Ошибка при расчёте метрик: {e}"}

        logger.debug(f"Были получены результаты метрик {metric_results}")
        if all(metric_results.values()):
            return {"success": True, "message": "", "result": metric_results}

        return {"success": False, "message": "Не были пройдены некоторые метрики", "result": metric_results}

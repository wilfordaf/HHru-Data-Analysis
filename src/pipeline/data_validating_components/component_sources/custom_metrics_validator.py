from typing import Any, Callable, Dict

import pandas as pd

from src import logger
from src.pipeline.data_validating_components.interfaces import IDataValidator


class CustomMetricsValidator(IDataValidator):
    def __init__(
        self,
        dataframe: pd.DataFrame,
        parameters: Dict[str, Any],
        custom_metrics: Dict[str, Callable[[pd.DataFrame, Dict[str, Any]], bool]],
    ) -> None:
        self._data = dataframe
        self._parameters = parameters
        self._custom_metrics = custom_metrics

    def validate_data(self) -> Dict[str, Any]:
        try:
            metric_results = {
                metric_name: metric(self._data, self._parameters)
                for metric_name, metric in self._custom_metrics.items()
            }
        except Exception as e:
            return {"success": False, "message": f"Ошибка при расчёте метрик: {e}"}

        logger.debug(f"Были получены результаты метрик {metric_results}")
        if all(metric_results.values()):
            return {"success": True, "message": "", "result": metric_results}

        return {"success": False, "message": "Не были пройдены некоторые метрики", "result": metric_results}

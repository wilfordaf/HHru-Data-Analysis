from typing import Any, Callable, Dict

import pandas as pd

from src import logger
from src.pipeline.data_validating_components.interfaces import IDataValidator


class CustomMetricsValidator(IDataValidator):
    """
    Класс для валидации данных с помощью самостоятельно реализованных метрик, критериев.
    Принимает в конструкторе:
    :param dataframe: данные, полученные на шаге предобработки, которые хотим валидировать.
    :param parameters: параметры датасета, получаются из DataController по DatasetName.
    :param custom_metrics: словарь метрик название: метод. Метод может быть как lambda-функцией, так и методом.
        :param dataframe: Датасет, который хотим валидировать метрикой. Подставляется self._data.
        :param parameters: Параметры датасета. Подставляется self._custom_metrics.
        :return: результат пройдена метрика или нет.

    Пример реализованной метрики:
    lambda d, p: d.shape[0] >= p["minimal_data_rows"]
    """

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

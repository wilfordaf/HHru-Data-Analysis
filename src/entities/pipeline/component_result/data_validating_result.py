from pydantic.dataclasses import dataclass

from src.entities.pipeline.component_result.validating_result import ValidatingResult
from src.enums import DatasetValidationError


@dataclass(config={"arbitrary_types_allowed": True})
class DataValidatingResult(ValidatingResult[DatasetValidationError]):
    """
    Класс для результата шага валидации данных.
    Может содержать ошибки:
    - PREPROCESSING_ERROR = Во время предобработки произошла ошибка, валидация не была проведена
    - BAD_DATA_QUALITY_ERROR = Датасет не прошёл простые проверки качества
    - CLASS_DISBALANCE_ERROR = Обнаружен значительный дисбаланс классов в выборке
    - DISTRIBUTION_DEVIATION_ERROR = Обнаружено значительное отклонение от целевого распределения
    """
